from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime, timedelta
import random
import math
import yfinance as yf

application = Flask(__name__)
CORS(application)

# ============================================
# NIFTY BREADTH DATA
# ============================================

@application.route('/api/breadth')
def get_breadth():
    return jsonify({
        'advances': 21,
        'declines': 29,
        'ad_ratio': 0.72,
        'index_price': 8691.40,
        'change': '+45.20',
        'change_percent': '+0.52',
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

# ============================================
# REAL-TIME NIFTY PRICE (Using yfinance)
# ============================================

@application.route('/api/realtime-nifty')
def get_realtime_nifty():
    try:
        nifty = yf.Ticker("^NSEI")
        data = nifty.history(period="1d")
        
        if data.empty:
            return jsonify({'error': 'Unable to fetch NIFTY data'}), 500
        
        current_price = round(data['Close'].iloc[-1], 2)
        open_price = round(data['Open'].iloc[-1], 2)
        change = round(current_price - open_price, 2)
        change_percent = round((change / open_price) * 100, 2)
        day_high = round(data['High'].max(), 2)
        day_low = round(data['Low'].min(), 2)
        volume = int(data['Volume'].iloc[-1])
        
        return jsonify({
            'symbol': 'NIFTY 50',
            'current_price': current_price,
            'open': open_price,
            'high': day_high,
            'low': day_low,
            'change': change,
            'change_percent': change_percent,
            'volume': volume,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        return jsonify({'error': str(e), 'message': 'Failed to fetch NIFTY data'}), 500

# ============================================
# BANK NIFTY REAL-TIME DATA
# ============================================

@application.route('/api/realtime-banknifty')
def get_realtime_banknifty():
    try:
        banknifty = yf.Ticker("^NSEBANK")
        data = banknifty.history(period="1d")
        
        if data.empty:
            return jsonify({'error': 'Unable to fetch BANKNIFTY data'}), 500
        
        current_price = round(data['Close'].iloc[-1], 2)
        open_price = round(data['Open'].iloc[-1], 2)
        change = round(current_price - open_price, 2)
        change_percent = round((change / open_price) * 100, 2)
        
        return jsonify({
            'symbol': 'BANK NIFTY',
            'current_price': current_price,
            'open': open_price,
            'change': change,
            'change_percent': change_percent,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================
# TOP GAINERS & LOSERS
# ============================================

@application.route('/api/movers')
def get_movers():
    try:
        symbols = ['RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS', 
                   'HINDUNILVR.NS', 'SBIN.NS', 'BHARTIARTL.NS', 'ITC.NS', 'LT.NS']
        
        gainers = []
        losers = []
        
        for symbol in symbols:
            stock = yf.Ticker(symbol)
            data = stock.history(period="1d")
            
            if not data.empty:
                current = data['Close'].iloc[-1]
                open_price = data['Open'].iloc[-1]
                change_percent = round(((current - open_price) / open_price) * 100, 2)
                
                stock_info = {
                    'symbol': symbol.replace('.NS', ''),
                    'price': round(current, 2),
                    'change_percent': change_percent
                }
                
                if change_percent > 0:
                    gainers.append(stock_info)
                else:
                    losers.append(stock_info)
        
        gainers.sort(key=lambda x: x['change_percent'], reverse=True)
        losers.sort(key=lambda x: x['change_percent'])
        
        return jsonify({
            'gainers': gainers[:5],
            'losers': losers[:5],
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================
# OPTIONS CHAIN DATA
# ============================================

@application.route('/api/options-chain')
def get_options_chain():
    spot_price = 8691.40
    strike_prices = [8450, 8500, 8550, 8600, 8650, 8700, 8750, 8800, 8850, 8900]
    
    calls = []
    puts = []
    
    for strike in strike_prices:
        call_iv = max(0, spot_price - strike)
        put_iv = max(0, strike - spot_price)
        
        calls.append({
            'strike': strike,
            'ltp': round(random.uniform(0.5, 150) + call_iv * 0.8, 2),
            'oi': random.randint(100000, 5000000),
            'change_oi': round(random.uniform(-15, 25), 2),
            'iv': round(random.uniform(12, 35), 2)
        })
        
        puts.append({
            'strike': strike,
            'ltp': round(random.uniform(0.5, 150) + put_iv * 0.8, 2),
            'oi': random.randint(100000, 5000000),
            'change_oi': round(random.uniform(-15, 25), 2),
            'iv': round(random.uniform(12, 35), 2)
        })
    
    return jsonify({
        'symbol': 'NIFTY',
        'expiry': (datetime.now() + timedelta(days=7)).strftime('%d-%b-%Y'),
        'spot_price': spot_price,
        'calls': calls,
        'puts': puts,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

# ============================================
# PUT CALL RATIO (PCR)
# ============================================

@application.route('/api/pcr')
def get_put_call_ratio():
    put_oi = 45200000
    call_oi = 36800000
    pcr = round(put_oi / call_oi, 2)
    
    if pcr > 1.5:
        sentiment = 'Extreme Bullish'
        signal = 'BUY'
    elif pcr > 1.2:
        sentiment = 'Bullish'
        signal = 'BUY'
    elif pcr < 0.8:
        sentiment = 'Bearish'
        signal = 'SELL'
    elif pcr < 0.5:
        sentiment = 'Extreme Bearish'
        signal = 'SELL'
    else:
        sentiment = 'Neutral'
        signal = 'HOLD'
    
    return jsonify({
        'put_oi': put_oi,
        'call_oi': call_oi,
        'pcr': pcr,
        'sentiment': sentiment,
        'signal': signal,
        'pcr_change': round(random.uniform(-0.1, 0.1), 2),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

# ============================================
# TRADING SIGNALS
# ============================================

@application.route('/api/trading-signals')
def get_trading_signals():
    pcr = 1.23
    ad_ratio = 0.72
    max_pain = 8750
    current_price = 8691.40
    
    score = 0
    signals = []
    
    if pcr > 1.2:
        score += 2
        signals.append({'indicator': 'PCR', 'signal': 'BULLISH', 'value': pcr})
    elif pcr < 0.8:
        score -= 2
        signals.append({'indicator': 'PCR', 'signal': 'BEARISH', 'value': pcr})
    else:
        signals.append({'indicator': 'PCR', 'signal': 'NEUTRAL', 'value': pcr})
    
    if ad_ratio > 1:
        score += 1
        signals.append({'indicator': 'Advance/Decline', 'signal': 'BULLISH', 'value': ad_ratio})
    elif ad_ratio < 0.7:
        score -= 1
        signals.append({'indicator': 'Advance/Decline', 'signal': 'BEARISH', 'value': ad_ratio})
    else:
        signals.append({'indicator': 'Advance/Decline', 'signal': 'NEUTRAL', 'value': ad_ratio})
    
    if current_price < max_pain:
        score += 1
        signals.append({'indicator': 'Max Pain', 'signal': 'EXPECT UPSIDE', 'value': max_pain})
    elif current_price > max_pain:
        score -= 1
        signals.append({'indicator': 'Max Pain', 'signal': 'EXPECT DOWNSIDE', 'value': max_pain})
    else:
        signals.append({'indicator': 'Max Pain', 'signal': 'AT MAX PAIN', 'value': max_pain})
    
    if score >= 2:
        recommendation = 'STRONG BUY (Call Options)'
        action = 'BUY CE'
        confidence = 'High'
    elif score >= 1:
        recommendation = 'BUY (Call Options)'
        action = 'BUY CE'
        confidence = 'Medium'
    elif score <= -2:
        recommendation = 'STRONG SELL (Put Options)'
        action = 'BUY PE'
        confidence = 'High'
    elif score <= -1:
        recommendation = 'SELL (Put Options)'
        action = 'BUY PE'
        confidence = 'Medium'
    else:
        recommendation = 'HOLD / WAIT FOR CLEAR SIGNAL'
        action = 'Stay Cash'
        confidence = 'Low'
    
    return jsonify({
        'overall_score': score,
        'recommendation': recommendation,
        'action': action,
        'confidence': confidence,
        'signals': signals,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

# ============================================
# HEALTH CHECK
# ============================================

@application.route('/api/health')
def health_check():
    return jsonify({
        'status': 'running',
        'version': '1.0.0',
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

# ============================================
# ROOT ENDPOINT
# ============================================

@application.route('/')
def home():
    return jsonify({
        'message': 'NIFTY Options Trading Dashboard API',
        'endpoints': ['/api/breadth', '/api/realtime-nifty', '/api/realtime-banknifty', '/api/movers', '/api/options-chain', '/api/pcr', '/api/trading-signals', '/api/health']
    })

# ============================================
# RUN THE APPLICATION
# ============================================

if __name__ == '__main__':
    application.run(debug=True, host='0.0.0.0', port=5000)