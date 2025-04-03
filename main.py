import os
import uuid
import hashlib
import threading
import time
import subprocess
import platform
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

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
    """
    Upload endpoint:
      - Checks for a file.
      - Saves the file.
      - Computes its hash to check for duplicates.
      - Schedules a deletion if no stream starts within 15 minutes.
    """
    if 'video' not in request.files:
        return jsonify({'error': 'No video file provided'}), 400

    file = request.files['video']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file and allowed_file(file.filename):
        # Generate a unique video id and filename.
        video_id = str(uuid.uuid4())
        ext = file.filename.rsplit('.', 1)[1].lower()
        filename = f"{email}_{video_id}.{ext}"
        save_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)

        try:
            file.save(save_path)

            # Compute file hash for duplicate checking.
            file_hash = compute_file_hash(save_path)
            if file_hash in video_hashes:
                # Duplicate found: remove new file and return existing video id.
                os.remove(save_path)
                existing_video_id = video_hashes[file_hash]
                return jsonify({
                    'message': 'Duplicate video',
                    'videoId': existing_video_id
                }), 200
            else:
                video_hashes[file_hash] = video_id

            # Schedule deletion in 15 minutes if stream not started.
            threading.Timer(15 * 60, lambda: delete_if_not_streamed(video_id, save_path)).start()

            # For this example, we use a dummy duration.
            duration = 120  # Dummy duration in seconds

            return jsonify({
                'message': 'File uploaded successfully',
                'videoId': video_id,
                'filename': filename,
                'duration': duration
            }), 200
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

def run_ffmpeg_stream(video_path, stream_key, loops, video_id, task_id):
    """
    Starts an FFmpeg stream in a new terminal window.
    Once the stream ends, the video file is removed (with a retry mechanism)
    and its hash is cleared so the same video can be re-uploaded.
    """
    try:
        print(f"[{datetime.now()}] Starting FFmpeg stream for '{video_path}'")
        # Determine output URL: if stream_key starts with 'rtmp://', use it as is.
        if stream_key.startswith("rtmp://"):
            output_url = stream_key
        else:
            output_url = f"rtmp://a.rtmp.youtube.com/live2/{stream_key}"

        # Construct the base FFmpeg command.
        base_cmd = (
            f'ffmpeg -re -stream_loop {loops} -i "{video_path}" '
            f'-c:v libx264 -preset veryfast -b:v 2500k -maxrate 2500k -bufsize 512k '
            f'-f flv {output_url}'
        )
        
        # Determine OS and build the terminal command.
        system_platform = platform.system()
        if system_platform == "Windows":
            # 'start cmd /k' opens a new command prompt and keeps it open.
            terminal_cmd = f'start cmd /c "{base_cmd}"'
        elif system_platform == "Linux":
            # Using gnome-terminal; adjust if you use a different terminal emulator.
            terminal_cmd = f'gnome-terminal -- bash -c \'{base_cmd}\''
        else:
            # For unsupported systems, run the command directly.
            terminal_cmd = base_cmd

        # Launch the command in a new terminal.
        process = subprocess.Popen(terminal_cmd, shell=True)
        
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
    required_fields = ['streamKey', 'loops', 'taskId']
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
        args=(video_path, data['streamKey'], data['loops'], video_id, data['taskId'])
    )
    thread.daemon = True
    thread.start()

    return jsonify({'message': 'Stream starting', 'videoId': video_id}), 200

@app.route('/stop/<video_id>', methods=['POST'])
def stop_stream(video_id):
    """
    Stop streaming endpoint:
      - Terminates the FFmpeg process for the given video_id and cleans up the stream record.
    """
    if video_id not in active_streams:
        return jsonify({'error': 'Stream not found'}), 404

    stream_data = active_streams[video_id]
    if stream_data.get('process'):
        cleanup_process(stream_data['process'])
    stream_data['status'] = 'stopped'
    stream_data['process'] = None
    active_streams.pop(video_id, None)
    return jsonify({'message': 'Stream stopped successfully'}), 200

@app.route('/streams/<video_id>', methods=['GET'])
def get_stream_status(video_id):
    """
    Get stream status endpoint:
      - Returns the current state of the stream (e.g. starting, live, completed, error).
    """
    stream = active_streams.get(video_id)
    if not stream:
        return jsonify({'error': 'Stream not found'}), 404

    if stream.get('process') and stream['process'].poll() is not None:
        stream['status'] = 'completed'
        stream['process'] = None
    return jsonify(stream), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
