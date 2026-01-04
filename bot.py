"""
Kalshi Risk-Neutralization Trading Bot
Implements "Free Roll" strategy: Sell enough contracts to recover initial capital when price increases 50%
"""

import os
import sys
import time
from datetime import datetime
from math import floor
from typing import Dict, List, Optional

import requests
from kalshi_python import KalshiClient, Configuration
from supabase import create_client, Client


class KalshiBot:
    """Main trading bot class for executing risk-neutralization strategy"""
    
    def __init__(self):
        """Initialize bot with API credentials and clients"""
        # 1. Load environment variables
        self.kalshi_key = os.environ.get('KALSHI_KEY')
        self.kalshi_secret = os.environ.get('KALSHI_SECRET')
        self.supabase_url = os.environ.get('SUPABASE_URL')
        self.supabase_key = os.environ.get('SUPABASE_KEY')
        self.discord_url = os.environ.get('DISCORD_URL')
        
        # 2. Validate required credentials
        self._validate_credentials()
        
        # 3. Initialize clients
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        self.kalshi_api = self._init_kalshi_client()
        
        print(f"[{self._timestamp()}] Bot initialized successfully")
    
    def _validate_credentials(self):
        """Ensure all required environment variables are present"""
        required = ['KALSHI_KEY', 'KALSHI_SECRET', 'SUPABASE_URL', 'SUPABASE_KEY', 'DISCORD_URL']
        missing = [var for var in required if not os.environ.get(var)]
        
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    def _init_kalshi_client(self) -> KalshiClient:
        """Initialize and authenticate Kalshi API client"""
        try:
            config = Configuration(
                host="https://demo-api.kalshi.co/trade-api/v2"
            )
            
            # Attach your credentials to the config
            config.api_key_id = self.kalshi_key
            config.private_key_pem = self.kalshi_secret  # FIXED: Use private_key_pem
            
            # Create the client with the configured credentials
            client = KalshiClient(configuration=config)
            
            print(f"[{self._timestamp()}] Kalshi client authenticated")
            return client
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Kalshi client: {str(e)}")
    
    def _timestamp(self) -> str:
        """Generate formatted timestamp for logging"""
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def fetch_open_positions(self) -> list:
        """
        AUTO-PILOT MODE:
        Fetches ALL open positions directly from your Kalshi portfolio.
        Filters out small bets to save cash.
        """
        # --- SETTINGS ---
        # Only hedge positions worth more than this $ amount
        MIN_INVESTMENT_TO_HEDGE = 10  
        # ----------------
        
        try:
            print(f"[{self._timestamp()}] Scanning Kalshi Portfolio...")
            
            # 1. Get real positions from Kalshi
            response = self.kalshi_api.get_positions()
            portfolio_items = response.market_positions
            
            valid_positions = []
            
            for p in portfolio_items:
                # Calculate how much you have invested (Quantity * Average Price)
                # Note: Kalshi prices are usually in cents (e.g., 50 = $0.50)
                # If price is < 1, we assume it's dollars already
                
                price_in_cents = p.avg_price
                if p.avg_price < 1: # Handle edge case if API returns dollars (e.g. 0.50)
                    price_in_cents = p.avg_price * 100
                    
                invested_value_dollars = (p.position * price_in_cents) / 100
                
                # 2. The Filter: Is this bet big enough to care about?
                if invested_value_dollars >= MIN_INVESTMENT_TO_HEDGE:
                    print(f" -> Tracking {p.ticker} (${invested_value_dollars:.2f} invested)")
                    
                    # Convert to the format our bot expects
                    valid_positions.append({
                        "ticker": p.ticker,
                        "entry_price": p.avg_price,
                        "quantity": p.position,
                        "status": "OPEN",
                        "id": None  # No database ID in auto-pilot mode
                    })
                else:
                    print(f" -> Ignoring {p.ticker} (Only ${invested_value_dollars:.2f} invested)")

            if not valid_positions:
                print(f"[{self._timestamp()}] No positions found above ${MIN_INVESTMENT_TO_HEDGE} threshold.")
                
            return valid_positions

        except Exception as e:
            print(f"Error fetching portfolio: {e}")
            return []
    
    def get_current_price(self, ticker: str) -> Optional[float]:
        """Fetch current yes_bid price from Kalshi API"""
        try:
            # Get market information
            market = self.kalshi_api.get_market(ticker=ticker)
            
            if not market or not hasattr(market, 'market'):
                print(f"[{self._timestamp()}] WARNING: No market data for {ticker}")
                return None
            
            # Extract yes_bid price (the price we can sell at)
            yes_bid = market.market.yes_bid / 100.0  # Kalshi prices are in cents
            print(f"[{self._timestamp()}] {ticker} current yes_bid: ${yes_bid:.2f}")
            return yes_bid
            
        except Exception as e:
            print(f"[{self._timestamp()}] ERROR fetching price for {ticker}: {str(e)}")
            return None
    
    def calculate_hedge_quantity(self, entry_price: float, quantity: int, current_price: float) -> int:
        """Calculate number of contracts to sell to recover initial capital"""
        initial_capital = entry_price * quantity
        contracts_to_sell = floor(initial_capital / current_price)
        
        # Safety bounds
        contracts_to_sell = max(0, min(contracts_to_sell, quantity - 1))
        
        print(f"[{self._timestamp()}] Hedge calculation: Initial capital=${initial_capital:.2f}, "
              f"Selling {contracts_to_sell}/{quantity} contracts")
        
        return contracts_to_sell
    
    def execute_sell_order(self, ticker: str, quantity: int, price: float) -> bool:
        """Place a limit sell order on Kalshi"""
        try:
            # Convert price back to cents for Kalshi API
            price_cents = int(price * 100)
            
            print(f"[{self._timestamp()}] Placing sell order: {quantity} contracts of {ticker} at ${price:.2f}")
            
            # Create sell order using Kalshi API
            order = self.kalshi_api.create_order(
                ticker=ticker,
                client_order_id=f"hedge_{ticker}_{int(time.time())}",
                side="sell",
                action="sell",
                count=quantity,
                type="limit",
                yes_price=price_cents
            )
            
            if order and hasattr(order, 'order'):
                order_id = order.order.order_id
                print(f"[{self._timestamp()}] ✅ Order placed successfully. Order ID: {order_id}")
                return True
            else:
                print(f"[{self._timestamp()}] ❌ Order placement failed - no order returned")
                return False
                
        except Exception as e:
            print(f"[{self._timestamp()}] ERROR executing sell order for {ticker}: {str(e)}")
            self.send_discord_alert(f"⚠️ Order Execution Error: {ticker} - {str(e)}")
            return False
    
    def update_position_status(self, position_id: int, remaining_quantity: int):
        """Update position in Supabase after hedge execution"""
        try:
            # Skip database update if no position ID (auto-pilot mode)
            if position_id is None:
                print(f"[{self._timestamp()}] Skipping database update (auto-pilot mode)")
                return
                
            update_data = {
                'status': 'HEDGED',
                'quantity': remaining_quantity
            }
            
            self.supabase.table('positions').update(update_data).eq('id', position_id).execute()
            print(f"[{self._timestamp()}] Updated position {position_id} in database")
            
        except Exception as e:
            print(f"[{self._timestamp()}] ERROR updating position {position_id}: {str(e)}")
            self.send_discord_alert(f"⚠️ Database Update Error: Position {position_id} - {str(e)}")
    
    def send_discord_alert(self, message: str):
        """Send notification to Discord webhook"""
        try:
            payload = {
                'content': message,
                'username': 'Kalshi Trading Bot'
            }
            
            response = requests.post(self.discord_url, json=payload, timeout=10)
            
            if response.status_code == 204:
                print(f"[{self._timestamp()}] Discord notification sent")
            else:
                print(f"[{self._timestamp()}] Discord notification failed: {response.status_code}")
                
        except Exception as e:
            print(f"[{self._timestamp()}] ERROR sending Discord alert: {str(e)}")
    
    def process_position(self, position: Dict):
        """Process a single position for potential hedge execution"""
        ticker = position['ticker']
        entry_price = float(position['entry_price'])
        quantity = int(position['quantity'])
        position_id = position.get('id')  # Use get() to handle None safely
        
        print(f"\n[{self._timestamp()}] Processing {ticker}: Entry=${entry_price:.2f}, Qty={quantity}")
        
        # Fetch current market price
        current_price = self.get_current_price(ticker)
        
        if current_price is None:
            print(f"[{self._timestamp()}] Skipping {ticker} - no price data available")
            return
        
        # Calculate gain percentage
        percent_gain = (current_price - entry_price) / entry_price
        print(f"[{self._timestamp()}] {ticker} gain: {percent_gain*100:.2f}%")
        
        # Check if trigger condition is met (50% gain)
        if percent_gain >= 0.50:
            print(f"[{self._timestamp()}] 🎯 TRIGGER MET for {ticker}! Executing hedge...")
            
            # Calculate contracts to sell
            contracts_to_sell = self.calculate_hedge_quantity(entry_price, quantity, current_price)
            
            if contracts_to_sell <= 0:
                print(f"[{self._timestamp()}] ⚠️ Invalid hedge quantity calculated, skipping")
                return
            
            # Execute sell order
            success = self.execute_sell_order(ticker, contracts_to_sell, current_price)
            
            if success:
                # Calculate remaining contracts
                remaining_quantity = quantity - contracts_to_sell
                capital_recovered = contracts_to_sell * current_price
                
                # Update database
                self.update_position_status(position_id, remaining_quantity)
                
                # Send success notification
                message = (
                    f"🟢 **HEDGE EXECUTED**\n"
                    f"📊 Ticker: {ticker}\n"
                    f"💰 Sold {contracts_to_sell} contracts at ${current_price:.2f}\n"
                    f"💵 Capital recovered: ${capital_recovered:.2f}\n"
                    f"🎁 Remaining {remaining_quantity} contracts are free profit!\n"
                    f"📈 Gain: {percent_gain*100:.1f}%"
                )
                self.send_discord_alert(message)
            else:
                print(f"[{self._timestamp()}] ❌ Hedge execution failed for {ticker}")
        else:
            print(f"[{self._timestamp()}] {ticker} below 50% threshold, no action taken")
    
    def run(self):
        """Main bot execution loop"""
        print(f"\n{'='*60}")
        print(f"[{self._timestamp()}] Starting Kalshi Risk-Neutralization Bot")
        print(f"{'='*60}\n")
        
        try:
            # Fetch all open positions
            positions = self.fetch_open_positions()
            
            if not positions:
                print(f"[{self._timestamp()}] No open positions to process")
                return
            
            # Process each position
            for position in positions:
                try:
                    self.process_position(position)
                except Exception as e:
                    ticker = position.get('ticker', 'UNKNOWN')
                    print(f"[{self._timestamp()}] ERROR processing {ticker}: {str(e)}")
                    self.send_discord_alert(f"⚠️ Error processing {ticker}: {str(e)}")
            
            print(f"\n[{self._timestamp()}] Bot execution completed successfully")
            
        except Exception as e:
            error_msg = f"Critical error in bot execution: {str(e)}"
            print(f"[{self._timestamp()}] {error_msg}")
            self.send_discord_alert(f"🚨 **CRITICAL ERROR**: {error_msg}")
            sys.exit(1)


def main():
    """Entry point for the bot"""
    try:
        bot = KalshiBot()
        bot.run()
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()