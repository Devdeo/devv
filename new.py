import os
import uuid
import hashlib
import threading
import time
import subprocess
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import asyncio
from pyppeteer import launch

app = Flask(__name__)
CORS(app)
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2GB limit

# -----------------------------
# Video Upload/Streaming Setup
# -----------------------------
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'mp4', 'mov', 'avi', 'mkv', 'webm'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

active_streams = {}
video_hashes = {}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def compute_file_hash(file_path):
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()

def delete_if_not_streamed(video_id, file_path):
    if video_id not in active_streams:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"[{datetime.now()}] File '{file_path}' deleted due to inactivity.")
                for file_hash, vid in list(video_hashes.items()):
                    if vid == video_id:
                        del video_hashes[file_hash]
        except Exception as e:
            print(f"Error deleting file: {e}")

@app.route('/upload/<email>', methods=['POST'])
def upload_video(email):
    if 'video' not in request.files:
        return jsonify({'error': 'No video file provided'}), 400
    file = request.files['video']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    if file and allowed_file(file.filename):
        video_id = str(uuid.uuid4())
        ext = file.filename.rsplit('.', 1)[1].lower()
        filename = f"{email}_{video_id}.{ext}"
        save_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        try:
            with open(save_path, 'wb') as f:
                while True:
                    chunk = file.stream.read(4096)
                    if not chunk:
                        break
                    f.write(chunk)
            response = jsonify({
                'message': 'File uploaded successfully',
                'videoId': video_id,
                'filename': filename,
                'duration': 120
            })
            response.status_code = 200

            def post_process():
                try:
                    file_hash = compute_file_hash(save_path)
                    if file_hash in video_hashes:
                        os.remove(save_path)
                        print(f"[{datetime.now()}] Duplicate detected. File removed.")
                    else:
                        video_hashes[file_hash] = video_id
                        threading.Timer(15 * 60, lambda: delete_if_not_streamed(video_id, save_path)).start()
                except Exception as e:
                    print(f"[{datetime.now()}] Post-process error: {e}")

            threading.Thread(target=post_process).start()
            return response
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    return jsonify({'error': 'Invalid file type'}), 400

# -----------------------------
# FFmpeg Streaming Functions
# -----------------------------
def cleanup_process(process):
    try:
        if process and process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
            if process.poll() is None:
                process.kill()
    except Exception as e:
        print(f"Error cleaning up process: {e}")

def run_ffmpeg_stream(video_path, stream_key, loops, video_id, task_id, platform):
    try:
        os.makedirs("logs", exist_ok=True)
        log_file = f"logs/{video_id}.log"
        ffmpeg_path = "/usr/local/bin/ffmpeg"
        ffmpeg_input = f'-re -stream_loop {loops} -i "{video_path}"'

        if platform == 'youtube':
            output_url = f'rtmp://a.rtmp.youtube.com/live2/{stream_key}'
            common_flags = '-c:v libx264 -preset veryfast -b:v 2500k -maxrate 2500k -bufsize 512k'
            format_flags = '-f flv'
        elif platform == 'facebook':
            output_url = f'rtmps://live-api-s.facebook.com:443/rtmp/{stream_key}'
            common_flags = ('-c:v libx264 -preset veryfast -b:v 4500k -minrate 4500k '
                            '-maxrate 4500k -bufsize 9000k -x264-params nal-hrd=cbr:force-cfr=1 '
                            '-c:a aac -b:a 128k -ar 44100 -ac 2')
            format_flags = '-f flv'
        elif platform == 'instagram':
            output_url = f'rtmps://edgetee-upload-del2-1.xx.fbcdn.net:443/rtmp/{stream_key}'
            common_flags = ('-c:v libx264 -preset veryfast -b:v 6000k -minrate 6000k '
                            '-maxrate 6000k -bufsize 12000k -x264-params nal-hrd=cbr:force-cfr=1 '
                            '-c:a aac -b:a 128k -ar 44100 -ac 2')
            format_flags = '-f flv'
        elif platform == 'twitter':
            output_url = f'rtmps://live.twitter.com/{stream_key}'
            common_flags = '-c:v libx264 -preset veryfast -b:v 2500k -maxrate 2500k -bufsize 5000k -c:a aac -b:a 128k -ar 44100 -ac 2'
            format_flags = '-f flv'
        else:
            return f"Unsupported platform: {platform}"

        base_cmd = f'{ffmpeg_path} {ffmpeg_input} {common_flags} {format_flags} "{output_url}"'

        with open(log_file, 'w') as log:
            process = subprocess.Popen(
                base_cmd,
                shell=True,
                stdout=log,
                stderr=subprocess.STDOUT,
                executable='/bin/bash'
            )

        active_streams[video_id] = {
            'process': process,
            'status': 'starting',
            'task_id': task_id,
            'start_time': datetime.now().isoformat()
        }

        time.sleep(5)
        active_streams[video_id]['status'] = 'live'
        process.wait()
        active_streams[video_id]['status'] = 'completed'

    except Exception as e:
        active_streams[video_id]['status'] = 'error'
        active_streams[video_id]['error'] = str(e)
        print(f"Error in run_ffmpeg_stream: {e}")
    finally:
        cleanup_process(process)
        time.sleep(2)
        attempts = 0
        max_attempts = 5
        while attempts < max_attempts:
            try:
                if os.path.exists(video_path):
                    os.remove(video_path)
                    print(f"[{datetime.now()}] File '{video_path}' removed after livestream ended.")
                    for file_hash, vid in list(video_hashes.items()):
                        if vid == video_id:
                            del video_hashes[file_hash]
                break
            except Exception as e:
                print(f"Error during post-stream cleanup (attempt {attempts+1}): {e}")
                attempts += 1
                time.sleep(2)
        threading.Timer(300, lambda: active_streams.pop(video_id, None)).start()

@app.route('/start/<video_id>', methods=['POST'])
def start_stream(video_id):
    data = request.get_json()
    required_fields = ['streamKey', 'loops', 'taskId', 'platform']
    if not all(field in data for field in required_fields):
        return jsonify({'error': 'Missing required fields'}), 400
    video_files = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if video_id in f]
    if not video_files:
        return jsonify({'error': 'Video file not found'}), 404
    video_path = os.path.join(app.config['UPLOAD_FOLDER'], video_files[0])
    thread = threading.Thread(
        target=run_ffmpeg_stream,
        args=(video_path, data['streamKey'], data['loops'], video_id, data['taskId'], data['platform'])
    )
    thread.daemon = True
    thread.start()
    return jsonify({'message': 'Stream starting', 'videoId': video_id}), 200

# -----------------------------
# NSE Index/Equity API Integration
# -----------------------------
index_cache = {}  # 30s cache
equity_cookie_cache = {'cookie': None, 'timestamp': 0}  # 5 min cache

# Index Option-Chain
@app.route('/nse-index', methods=['GET'])
def nse_index():
    symbol = request.args.get('symbol', 'NIFTY').upper()
    now = time.time()
    if symbol in index_cache and now - index_cache[symbol]['timestamp'] < 30:
        return jsonify(index_cache[symbol]['data'])
    url = f'https://www.nseindia.com/api/option-chain-indices?symbol={symbol}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://www.nseindia.com/option-chain'
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        index_cache[symbol] = {'data': data, 'timestamp': now}
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Equity Option-Chain
async def get_nse_cookies():
    now = time.time()
    if equity_cookie_cache['cookie'] and now - equity_cookie_cache['timestamp'] < 300:
        return equity_cookie_cache['cookie']
    browser = await launch(headless=True, args=['--no-sandbox'])
    page = await browser.newPage()
    await page.goto('https://www.nseindia.com', {'waitUntil': 'networkidle2'})
    cookies = await page.cookies()
    await browser.close()
    cookie_header = '; '.join([f"{c['name']}={c['value']}" for c in cookies])
    equity_cookie_cache['cookie'] = cookie_header
    equity_cookie_cache['timestamp'] = now
    return cookie_header

def fetch_equity_option_chain(symbol, cookie_header):
    url = f'https://www.nseindia.com/api/option-chain-equities?symbol={symbol}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://www.nseindia.com',
        'Cookie': cookie_header
    }
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    return response.json()

@app.route('/nse-equity', methods=['GET'])
def nse_equity():
    symbol = request.args.get('symbol', '').upper()
    if not symbol:
        return jsonify({'error': 'Symbol is required'}), 400
    try:
        cookie_header = asyncio.get_event_loop().run_until_complete(get_nse_cookies())
        data = fetch_equity_option_chain(symbol, cookie_header)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# -----------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
  
