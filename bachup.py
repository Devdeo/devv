import os
import uuid
import hashlib
import threading
import time
import subprocess
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2GB limit

# Configuration
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'mp4', 'mov', 'avi', 'mkv', 'webm'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# In-memory storage for active streams and file hashes.
active_streams = {}
video_hashes = {}

def allowed_file(filename):
    """Check whether the filename has an allowed extension."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def compute_file_hash(file_path):
    """Compute the SHA-256 hash of the file."""
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()

def delete_if_not_streamed(video_id, file_path):
    """
    After a delay, if the video hasn't been started for streaming,
    delete the file and remove its hash from video_hashes.
    """
    if video_id not in active_streams:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"[{datetime.now()}] File '{file_path}' deleted due to inactivity.")
                # Remove the hash corresponding to video_id.
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
            print(f"[{datetime.now()}] Saving file to {save_path}")
            with open(save_path, 'wb') as f:
                while True:
                    chunk = file.stream.read(4096)
                    if not chunk:
                        break
                    f.write(chunk)

            print(f"[{datetime.now()}] File saved successfully")

            # Respond early before expensive processing
            response = jsonify({
                'message': 'File uploaded successfully',
                'videoId': video_id,
                'filename': filename,
                'duration': 120  # dummy or replace with actual later
            })
            response.status_code = 200

            # Background task: hashing and duplicate check
            def post_process():
                try:
                    file_hash = compute_file_hash(save_path)
                    if file_hash in video_hashes:
                        print(f"[{datetime.now()}] Duplicate detected. Removing file.")
                        os.remove(save_path)
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

def cleanup_process(process):
    """Terminate and kill a process if still running."""
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
        print(f"[{datetime.now()}] Starting FFmpeg stream for '{video_path}'")
        # Determine output URL: if stream_key starts with 'rtmp://', use it as is.
        os.makedirs("logs", exist_ok=True)
        log_file = f"logs/{video_id}.log"
        # Construct the base FFmpeg command.
        ffmpeg_path = "/usr/local/bin/ffmpeg"
        ffmpeg_input = f'-re -stream_loop {loops} -i "{video_path}"'
        base_flags = (
        '-fflags +nobuffer -analyzeduration 2147483647 -probesize 2147483647'
        )
        if platform == 'youtube':
            output_url = f'rtmp://a.rtmp.youtube.com/live2/{stream_key}'        
            common_flags = (
                '-c:v libx264 -preset veryfast -b:v 2500k -maxrate 2500k -bufsize 512k'
           )
            format_flags = '-f flv'

        elif platform == 'facebook':
            output_url = f'rtmps://live-api-s.facebook.com:443/rtmp/{stream_key}'
            common_flags = (
                '-c:v libx264 -preset veryfast '
                '-b:v 4500k -minrate 4500k -maxrate 4500k -bufsize 9000k '
                '-x264-params nal-hrd=cbr:force-cfr=1 '
                '-c:a aac -b:a 128k -ar 44100 -ac 2'
            )
            format_flags = '-f flv'

        elif platform == 'instagram':
            output_url = f'rtmps://edgetee-upload-del2-1.xx.fbcdn.net:443/rtmp/{stream_key}'
            common_flags = (
                '-c:v libx264 -preset veryfast '
                '-b:v 6000k -minrate 6000k -maxrate 6000k -bufsize 12000k '
                '-x264-params nal-hrd=cbr:force-cfr=1 '
                '-c:a aac -b:a 128k -ar 44100 -ac 2'
            )
            format_flags = '-f flv'


        elif platform == 'twitter':
            output_url = f'rtmps://live.twitter.com/{stream_key}'
            common_flags = '-c:v libx264 -preset veryfast -b:v 2500k -maxrate 2500k -bufsize 5000k -c:a aac -b:a 128k -ar 44100 -ac 2'
            format_flags = '-f flv'

        else:
            return f"Unsupported platform: {platform}"

        base_cmd = f'{ffmpeg_path} {ffmpeg_input} {common_flags} {base_flags} {format_flags} "{output_url}"'


        # Run FFmpeg and log output
        with open(log_file, 'w') as log:
            process = subprocess.Popen(
                base_cmd,
                shell=True,
                stdout=log,
                stderr=subprocess.STDOUT,
                executable='/bin/bash'  # Ensure Bash is used
            )
        
        # Save process info in active_streams.
        active_streams[video_id] = {
            'process': process,
            'status': 'starting',
            'task_id': task_id,
            'start_time': datetime.now().isoformat()
        }
        
        # Optionally, wait a few seconds to let the process initialize.
        time.sleep(5)
        active_streams[video_id]['status'] = 'live'

        # Here, since FFmpeg is running in a separate terminal, monitoring its output is limited.
        # We'll simply wait for the process to complete.
        process.wait()
        active_streams[video_id]['status'] = 'completed'
    except Exception as e:
        active_streams[video_id]['status'] = 'error'
        active_streams[video_id]['error'] = str(e)
        print(f"Error in run_ffmpeg_stream: {e}")
    finally:
        cleanup_process(process)
        # Wait a bit to give the OS time to release the file.
        time.sleep(2)
        # Remove the video file after the stream has ended using a retry loop.
        attempts = 0
        max_attempts = 5
        while attempts < max_attempts:
            try:
                if os.path.exists(video_path):
                    os.remove(video_path)
                    print(f"[{datetime.now()}] File '{video_path}' removed after livestream ended.")
                    # Remove the hash corresponding to video_id.
                    for file_hash, vid in list(video_hashes.items()):
                        if vid == video_id:
                            del video_hashes[file_hash]
                break  # Exit loop if deletion is successful.
            except Exception as e:
                print(f"Error during post-stream cleanup (attempt {attempts+1}): {e}")
                attempts += 1
                time.sleep(2)
        # Optionally, remove the stream record after a delay.
        threading.Timer(300, lambda: active_streams.pop(video_id, None)).start()

@app.route('/start/<video_id>', methods=['POST'])
def start_stream(video_id):
    """
    Start streaming endpoint:
      - Expects JSON with keys: streamKey, loops, taskId.
      - Finds the video file containing the video_id.
      - Starts the streaming (in a background thread) using run_ffmpeg_stream.
    """
    data = request.get_json()
    required_fields = ['streamKey', 'loops', 'taskId', 'platform']
    if not all(field in data for field in required_fields):
        return jsonify({'error': 'Missing required fields'}), 400

    # Find the video file (assumes filename contains the video_id).
    video_files = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if video_id in f]
    if not video_files:
        return jsonify({'error': 'Video file not found'}), 404

    video_path = os.path.join(app.config['UPLOAD_FOLDER'], video_files[0])
    # Start the FFmpeg streaming in a background thread.
    thread = threading.Thread(
        target=run_ffmpeg_stream,
        args=(video_path, data['streamKey'], data['loops'], video_id, data['taskId'], data['platform'])
    )
    thread.daemon = True
    thread.start()

    return jsonify({'message': 'Stream starting', 'videoId': video_id}), 200

