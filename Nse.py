from flask import Flask, jsonify, request
import requests
import time

app = Flask(__name__)

index_cache = {}
equity_cache = {}

# --- NSE Index Option Chain ---
@app.route('/nse-index', methods=['GET'])
def nse_index():
    symbol = request.args.get('symbol', 'NIFTY').upper()
    now = time.time()

    # 30 sec cache
    if symbol in index_cache and now - index_cache[symbol]['timestamp'] < 30:
        return jsonify(index_cache[symbol]['data'])

    url = f'https://www.nseindia.com/api/option-chain-indices?symbol={symbol}'
    try:
        data = requests.get(url, timeout=10).json()
        index_cache[symbol] = {'data': data, 'timestamp': now}
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# --- NSE Equity Option Chain ---
@app.route('/nse-equity', methods=['GET'])
def nse_equity():
    symbol = request.args.get('symbol', '').upper()
    if not symbol:
        return jsonify({'error': 'Symbol is required'}), 400

    now = time.time()
    if symbol in equity_cache and now - equity_cache[symbol]['timestamp'] < 30:
        return jsonify(equity_cache[symbol]['data'])

    url = f'https://www.nseindia.com/api/option-chain-equities?symbol={symbol}'
    try:
        data = requests.get(url, timeout=10).json()
        equity_cache[symbol] = {'data': data, 'timestamp': now}
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
          
