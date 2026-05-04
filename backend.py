# ============================================
# PROFESSIONAL SCALPING SIGNALS ENDPOINT
# Features: Step-up stop loss, 2% loss exit, 2.3% take profit
# ============================================

@application.route('/api/trading-signals')
def get_trading_signals():
    try:
        # Fetch intraday data (1-minute intervals)
        nifty = yf.Ticker("^NSEI")
        data = nifty.history(period="1d", interval="1m")
        
        # Fallback to 5-min if 1-min not available
        if data.empty or len(data) < 10:
            data = nifty.history(period="1d", interval="5m")
        
        if data.empty:
            return jsonify({'error': 'No data available'}), 500
        
        # ============================================
        # 1. CALCULATE CORE METRICS
        # ============================================
        
        current_price = round(data['Close'].iloc[-1], 2)
        current_volume = int(data['Volume'].iloc[-1])
        avg_volume = int(data['Volume'].tail(20).mean())
        volume_ratio = round(current_volume / avg_volume, 2) if avg_volume > 0 else 1
        
        # Calculate 3-minute momentum (last 3 candles)
        if len(data) >= 4:
            price_3min_ago = data['Close'].iloc[-4]
            momentum_3min = round(current_price - price_3min_ago, 2)
            momentum_percent = round((momentum_3min / price_3min_ago) * 100, 2)
        else:
            momentum_3min = 0
            momentum_percent = 0
        
        # Calculate intraday high/low
        open_price = round(data['Open'].iloc[0], 2)
        day_high = round(data['High'].max(), 2)
        day_low = round(data['Low'].min(), 2)
        intraday_range = round(day_high - day_low, 2)
        
        # Calculate ATR for dynamic stops
        high_low = data['High'] - data['Low']
        high_close = abs(data['High'] - data['Close'].shift())
        low_close = abs(data['Low'] - data['Close'].shift())
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = round(true_range.tail(14).mean(), 2)
        
        # Calculate RSI
        delta = data['Close'].diff()
        gain = (delta.where(delta > 0, 0)).tail(14).mean()
        loss = (-delta.where(delta < 0, 0)).tail(14).mean()
        rs = gain / loss if loss != 0 else 1
        rsi = round(100 - (100 / (1 + rs)), 1)
        
        # ============================================
        # 2. TRACK POSITION (Simulated for step-up stop loss)
        # ============================================
        
        # Simulate if we had an active position (in real trading, you'd track this per user)
        # Here we calculate hypothetical step-up stop based on today's high/low
        
        hypothetical_entry = open_price
        hypothetical_highest = day_high
        hypothetical_lowest = day_low
        
        # Calculate step-up stop loss (trailing stop)
        # Rule: Stop loss moves up 2% below the highest price achieved
        step_up_stop = round(hypothetical_highest * 0.98, 2)  # 2% below highest
        hard_stop_loss = round(hypothetical_entry * 0.98, 2)  # 2% below entry
        take_profit = round(hypothetical_entry * 1.023, 2)     # 2.3% above entry
        
        # Determine if step-up stop is active (price has moved up significantly)
        is_step_up_active = current_price > hypothetical_entry * 1.02  # Up 2% or more
        active_stop_loss = step_up_stop if is_step_up_active else hard_stop_loss
        
        # Check if we should exit based on step-up stop
        exit_signal = None
        if current_price <= active_stop_loss:
            exit_signal = {
                "signal": "EXIT - Stop Loss Hit",
                "reason": f"Price fell to stop loss level {active_stop_loss}",
                "action": "CLOSE POSITION"
            }
        elif current_price >= take_profit:
            exit_signal = {
                "signal": "TAKE PROFIT - 2.3% Target Achieved",
                "reason": f"Price reached target {take_profit}",
                "action": "BOOK PROFIT"
            }
        
        # ============================================
        # 3. SIGNAL GENERATION (ONLY IF NO ACTIVE STEP-UP STOP)
        # ============================================
        
        action = "HOLD"
        recommendation = "No clear signal"
        confidence = "Low"
        overall_score = 0
        trade_type = None
        entry_price = current_price
        trigger_reason = "Awaiting 3-minute momentum >8 points with volume"
        
        # ONLY generate new signals if we don't have an active step-up stop scenario
        if not is_step_up_active and not exit_signal:
            
            # SIGNAL: BUY CE - Price rises >8 points in 3 minutes with volume
            if momentum_3min > 8 and volume_ratio > 1.2 and rsi < 70:
                action = "BUY CE"
                recommendation = "📈 BUY CALL OPTION - Strong upside momentum"
                confidence = "High" if momentum_3min > 15 else "Medium"
                overall_score = 2
                trade_type = "LONG_CALL"
                trigger_reason = f"Rose {momentum_3min} points in 3 min | Volume {volume_ratio}x avg"
                
            # SIGNAL: BUY PE - Price falls >8 points in 3 minutes with volume
            elif momentum_3min < -8 and volume_ratio > 1.2 and rsi > 30:
                action = "BUY PE"
                recommendation = "📉 BUY PUT OPTION - Strong downside momentum"
                confidence = "High" if momentum_3min < -15 else "Medium"
                overall_score = -2
                trade_type = "LONG_PUT"
                trigger_reason = f"Fell {abs(momentum_3min)} points in 3 min | Volume {volume_ratio}x avg"
            
            # SIGNAL: SHORT CE - Overbought + reversal
            elif rsi > 75 and momentum_3min < -3 and volume_ratio > 1.1:
                action = "SHORT CE"
                recommendation = "🔻 SHORT CALL OPTION - Overbought reversal expected"
                confidence = "Medium"
                overall_score = -1
                trade_type = "SHORT_CALL"
                trigger_reason = f"RSI {rsi} (Overbought) + falling {abs(momentum_3min)} points"
            
            # SIGNAL: SHORT PE - Oversold + reversal
            elif rsi < 25 and momentum_3min > 3 and volume_ratio > 1.1:
                action = "SHORT PE"
                recommendation = "🔺 SHORT PUT OPTION - Oversold reversal expected"
                confidence = "Medium"
                overall_score = 1
                trade_type = "SHORT_PUT"
                trigger_reason = f"RSI {rsi} (Oversold) + rising {momentum_3min} points"
        
        # If step-up stop is active, show HOLD with trailing stop info
        elif is_step_up_active and not exit_signal:
            action = "HOLD"
            recommendation = f"🔒 HOLD - Position active with step-up stop at ₹{active_stop_loss}"
            confidence = "Medium"
            overall_score = 0
            trigger_reason = f"Trailing stop active. Stop loss stepped up to {active_stop_loss}"
        
        # ============================================
        # 4. EXIT TABLE (Step-Up Stop, Hard Stop, Take Profit)
        # ============================================
        
        exit_table = []
        
        if trade_type in ["LONG_CALL", "SHORT_PUT"]:
            # Bullish trades
            exit_table = [
                {"level": "Hard Stop Loss (2%)", "price": round(entry_price * 0.98, 2), 
                 "action": "EXIT - Maximum loss 2%", "priority": 1},
                {"level": "Take Profit (2.3%)", "price": round(entry_price * 1.023, 2), 
                 "action": "BOOK PROFIT - Target achieved", "priority": 2},
                {"level": "Step-Up Stop Loss", "price": "Trailing", 
                 "action": f"Moves up 2% below highest price. Current: ₹{hypothetical_highest * 0.98:.2f}", 
                 "priority": 3}
            ]
        elif trade_type in ["LONG_PUT", "SHORT_CALL"]:
            # Bearish trades
            exit_table = [
                {"level": "Hard Stop Loss (2%)", "price": round(entry_price * 1.02, 2), 
                 "action": "EXIT - Maximum loss 2%", "priority": 1},
                {"level": "Take Profit (2.3%)", "price": round(entry_price * 0.977, 2), 
                 "action": "BOOK PROFIT - Target achieved", "priority": 2},
                {"level": "Step-Up Stop Loss", "price": "Trailing", 
                 "action": f"Moves down 2% above lowest price. Current: ₹{hypothetical_lowest * 1.02:.2f}", 
                 "priority": 3}
            ]
        else:
            # No active trade - show hypothetical exit plan
            exit_table = [
                {"level": "Hard Stop Loss (2%)", "price": round(current_price * 0.98, 2), 
                 "action": "EXIT - Cut losses at 2%", "priority": 1},
                {"level": "Take Profit (2.3%)", "price": round(current_price * 1.023, 2), 
                 "action": "BOOK PROFIT - Target 2.3% gain", "priority": 2},
                {"level": "Step-Up Stop Loss", "price": "Trailing", 
                 "action": "Lock in profits by trailing stop 2% below peak", "priority": 3}
            ]
        
        # ============================================
        # 5. COMPLETE RESPONSE
        # ============================================
        
        return jsonify({
            # Signal Information
            'action': action,
            'recommendation': recommendation,
            'confidence': confidence,
            'overall_score': overall_score,
            'trade_type': trade_type,
            'trigger_reason': trigger_reason,
            
            # Exit Signals (CRITICAL NEW FEATURES)
            'exit_signal': exit_signal,
            'is_step_up_stop_active': is_step_up_active,
            'active_stop_loss': active_stop_loss if is_step_up_active else hard_stop_loss,
            
            # Entry & Exit Levels
            'entry_price': entry_price if trade_type else None,
            'hard_stop_loss_2_percent': round(hard_stop_loss, 2),
            'take_profit_2_3_percent': round(take_profit, 2),
            'step_up_stop_price': round(step_up_stop, 2) if is_step_up_active else None,
            'exit_table': exit_table,
            
            # Position Tracking
            'highest_price_reached': hypothetical_highest,
            'lowest_price_reached': hypothetical_lowest,
            'unrealized_pnl_percent': round(((current_price / hypothetical_entry) - 1) * 100, 2),
            
            # Market Context
            'spot_price': current_price,
            'open_price': open_price,
            'day_high': day_high,
            'day_low': day_low,
            'intraday_range': intraday_range,
            
            # Momentum & Volume
            'momentum_3min_points': momentum_3min,
            'momentum_3min_percent': momentum_percent,
            'volume_ratio': volume_ratio,
            
            # Technical Indicators
            'atr_14': atr,
            'rsi_14': rsi,
            
            # Timestamp
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
    except Exception as e:
        print(f"Error in trading signals: {e}")
        return jsonify({'error': str(e), 'fallback': True}), 500