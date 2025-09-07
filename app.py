import os
import math
import json
import subprocess
import threading
import time
from flask import Flask, render_template, send_file, jsonify, abort, request, Response
import logging
from pathlib import Path
import re
from datetime import datetime
import uuid
import shutil

app = Flask(__name__)

# Logging configuration (default INFO, override via LOG_LEVEL env)
log_level_name = os.environ.get('LOG_LEVEL', 'INFO').upper()
level = getattr(logging, log_level_name, logging.INFO)
logging.basicConfig(level=level, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger('reodash')

# Configuration
RECORDINGS_PATH = os.environ.get('RECORDINGS_PATH', '/recordings')
MAX_CONCURRENT_TRANSCODES = 3
HLS_PATH = os.environ.get('HLS_PATH', '/tmp/reodash_hls')
Path(HLS_PATH).mkdir(parents=True, exist_ok=True)

# Global transcoding state
active_transcodes = 0
transcode_lock = threading.Lock()

# Track active HLS jobs for cleanup 
hls_jobs = {}
hls_jobs_lock = threading.Lock()

def can_start_transcode():
    """Check if we can start a new transcoding job"""
    global active_transcodes
    with transcode_lock:
        return active_transcodes < MAX_CONCURRENT_TRANSCODES

def increment_transcodes():
    """Safely increment active transcodes counter"""
    global active_transcodes
    with transcode_lock:
        active_transcodes += 1

def decrement_transcodes():
    """Safely decrement active transcodes counter"""
    global active_transcodes
    with transcode_lock:
        active_transcodes -= 1

"""Removed legacy direct MP4 streaming and caching helpers in favor of HLS."""

def parse_filename(filename):
    """Parse recording filename to extract metadata"""
    # Pattern: Driveway_00_20250905173157.jpg/mp4
    pattern = r'(.+)_(\d{2})_(\d{14})\.(jpg|mp4)'
    match = re.match(pattern, filename)
    if match:
        camera_name, sequence, timestamp_str, extension = match.groups()
        timestamp = datetime.strptime(timestamp_str, '%Y%m%d%H%M%S')
        return {
            'camera_name': camera_name,
            'sequence': sequence,
            'timestamp': timestamp,
            'extension': extension,
            'base_name': f"{camera_name}_{sequence}_{timestamp_str}"
        }
    return None

def get_file_tree():
    """Build file tree structure from recordings directory"""
    tree = {}
    # Prepare a top-level "Today" node aggregating all cameras' recordings for today's date
    today = datetime.today()
    today_year = f"{today.year:04d}"
    today_month = f"{today.month:02d}"
    today_day = f"{today.day:02d}"
    tree["Today"] = []
    recordings_path = Path(RECORDINGS_PATH)
    
    if not recordings_path.exists():
        return tree
    
    for camera_dir in recordings_path.iterdir():
        if not camera_dir.is_dir():
            continue
            
        camera_name = camera_dir.name
        tree[camera_name] = {}
        
        for year_dir in camera_dir.iterdir():
            if not year_dir.is_dir():
                continue
                
            year = year_dir.name
            tree[camera_name][year] = {}
            
            for month_dir in year_dir.iterdir():
                if not month_dir.is_dir():
                    continue
                    
                month = month_dir.name
                tree[camera_name][year][month] = {}
                
                for day_dir in month_dir.iterdir():
                    if not day_dir.is_dir():
                        continue
                        
                    day = day_dir.name
                    recordings = []
                    
                    # Group files by base name (same recording)
                    files_by_base = {}
                    for file_path in day_dir.iterdir():
                        if file_path.is_file():
                            parsed = parse_filename(file_path.name)
                            if parsed:
                                base_name = parsed['base_name']
                                if base_name not in files_by_base:
                                    files_by_base[base_name] = {'jpg': None, 'mp4': None}
                                files_by_base[base_name][parsed['extension']] = file_path.name
                    
                    # Convert to list of recordings - prioritize MP4 files, show thumbnail if available
                    for base_name, files in files_by_base.items():
                        if files['mp4']:  # Only need MP4 file
                            mp4_path = day_dir / files['mp4']
                            if mp4_path.exists() and mp4_path.stat().st_size > 0:
                                # Check if JPG exists and is valid
                                thumbnail = None
                                if files['jpg']:
                                    jpg_path = day_dir / files['jpg']
                                    if jpg_path.exists() and jpg_path.stat().st_size > 0:
                                        thumbnail = files['jpg']
                                
                                recording_entry = {
                                    'base_name': base_name,
                                    'thumbnail': thumbnail,  # Can be None
                                    'video': files['mp4'],
                                    'path': f"{camera_name}/{year}/{month}/{day}"
                                }
                                recordings.append(recording_entry)
                                # If this recording is from today, also add it under the top-level Today bucket
                                if year == today_year and month == today_month and day == today_day:
                                    tree["Today"].append(recording_entry)
                    
                    # Sort by timestamp
                    recordings.sort(key=lambda x: x['base_name'])
                    tree[camera_name][year][month][day] = recordings
    
    return tree

@app.route('/')
def index():
    """Main page with file tree navigation"""
    return render_template('index.html')

@app.route('/api/tree')
def api_tree():
    """API endpoint to get file tree structure"""
    return jsonify(get_file_tree())

def get_range_requests(file_size):
    """Parse Range header and return start, end positions"""
    range_header = request.headers.get('Range', None)
    if not range_header:
        return None, None
    
    # Parse range header like "bytes=0-1023"
    match = re.search(r'bytes=(\d+)-(\d*)', range_header)
    if match:
        byte_start = int(match.group(1))
        byte_end = int(match.group(2)) if match.group(2) else file_size - 1
        return byte_start, min(byte_end, file_size - 1)
    
    return None, None

def start_hls_vod(input_path, fast_mode=True, playlist_type='event'):
    """Create HLS playlist and segments; return subpath. playlist_type: 'event' (streaming) or 'vod' (accurate)."""
    if not can_start_transcode():
        return None
    job_id = uuid.uuid4().hex
    job_dir = os.path.join(HLS_PATH, job_id)
    Path(job_dir).mkdir(parents=True, exist_ok=True)

    # Probe input to decide copy-vs-transcode per stream
    def probe_stream(select):
        try:
            result = subprocess.run([
                'ffprobe', '-v', 'error',
                '-select_streams', select,
                '-show_entries', 'stream=codec_name,pix_fmt',
                '-of', 'json', input_path
            ], capture_output=True, text=True, timeout=5)
            if result.returncode != 0:
                return None
            data = json.loads(result.stdout)
            streams = data.get('streams') or []
            return streams[0] if streams else None
        except Exception:
            return None

    v = probe_stream('v:0')
    a = probe_stream('a:0')
    v_codec = (v.get('codec_name') if v else '') or ''
    v_pix = (v.get('pix_fmt') if v else '') or ''
    a_codec = (a.get('codec_name') if a else '') or ''

    can_copy_video = v_codec.lower() == 'h264' and v_pix.lower() in ('yuv420p', 'yuvj420p')
    can_copy_audio = a_codec.lower() == 'aac'

    # CMAF/fMP4 HLS for broad compatibility; choose codecs based on probe
    cmd = ['ffmpeg', '-y', '-loglevel', 'error', '-fflags', '+genpts', '-i', input_path]

    if can_copy_video:
        cmd += ['-c:v', 'copy']
    else:
        cmd += [
            '-c:v', 'libx264',
            '-preset', 'veryfast' if fast_mode else 'medium',
            '-pix_fmt', 'yuv420p',
            '-profile:v', 'baseline',
            '-level', '3.0',
            '-vf', 'scale=720:-2' if fast_mode else 'scale=1280:-2',
            '-g', '48', '-sc_threshold', '0', '-keyint_min', '48', '-r', '24'
        ]

    if can_copy_audio:
        cmd += ['-c:a', 'copy']
    else:
        cmd += ['-c:a', 'aac', '-b:a', '96k' if fast_mode else '128k']

    # We'll have ffmpeg write to an internal playlist path while we publish a static VOD playlist
    internal_playlist = os.path.join(job_dir, 'internal.m3u8')
    cmd += [
        '-f', 'hls',
        '-hls_time', '2', '-hls_list_size', '0', '-hls_playlist_type', playlist_type,
        '-hls_segment_type', 'fmp4', '-hls_flags', 'independent_segments+append_list+temp_file',
        '-start_number', '0',
        '-hls_segment_filename', os.path.join(job_dir, 'seg_%05d.m4s'),
        '-hls_fmp4_init_filename', 'init.mp4',
        internal_playlist
    ]
    # Pre-generate a static VOD playlist with accurate duration
    published_playlist = os.path.join(job_dir, 'index.m3u8')
    segment_time = 2.0
    total_duration = None
    try:
        r = subprocess.run([
            'ffprobe', '-v', 'error', '-show_entries', 'format=duration',
            '-of', 'default=nk=1:nw=1', input_path
        ], capture_output=True, text=True, timeout=5)
        if r.returncode == 0:
            ds = (r.stdout or '').strip()
            if ds:
                total_duration = float(ds)
    except Exception:
        total_duration = None

    if total_duration and total_duration > 0:
        num_full = int(total_duration // segment_time)
        remainder = total_duration - (num_full * segment_time)
        segment_count = num_full + (1 if remainder > 0.01 else 0)
        target_duration = int(math.ceil(max(segment_time, remainder if remainder > 0 else segment_time)))
        lines = [
            '#EXTM3U',
            '#EXT-X-VERSION:7',
            f'#EXT-X-TARGETDURATION:{target_duration}',
            '#EXT-X-MEDIA-SEQUENCE:0',
            '#EXT-X-PLAYLIST-TYPE:VOD',
            '#EXT-X-INDEPENDENT-SEGMENTS',
            '#EXT-X-MAP:URI="init.mp4"'
        ]
        for i in range(segment_count):
            dur = segment_time if i < num_full else (remainder if remainder > 0.01 else segment_time)
            lines.append(f'#EXTINF:{dur:.3f},')
            lines.append(f'seg_{i:05d}.m4s')
        lines.append('#EXT-X-ENDLIST')
        try:
            with open(published_playlist, 'w') as f:
                f.write('\n'.join(lines) + '\n')
        except Exception:
            pass

    logger.debug(f"Starting HLS VOD job {job_id} for: {input_path}")
    increment_transcodes()
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Register job for later cleanup
    with hls_jobs_lock:
        hls_jobs[job_id] = {
            'proc': proc,
            'dir': job_dir
        }

    def finalize_cleanup(p, job_id_param, job_dir_param):
        try:
            p.wait(timeout=180)
        except subprocess.TimeoutExpired:
            p.terminate()
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill(); p.wait()
        finally:
            decrement_transcodes()
            # Remove ephemeral artifacts if still present
            with hls_jobs_lock:
                hls_jobs.pop(job_id_param, None)
            try:
                if os.path.isdir(job_dir_param):
                    shutil.rmtree(job_dir_param, ignore_errors=True)
            except Exception:
                pass
    # Wait for init.mp4 and first segment to exist for fast start
    init_path = os.path.join(job_dir, 'init.mp4')
    first_seg = os.path.join(job_dir, 'seg_00000.m4s')
    for _ in range(400):  # up to ~20s
        if os.path.exists(init_path) and os.path.exists(first_seg):
            break
        time.sleep(0.05)

    threading.Thread(target=finalize_cleanup, args=(proc, job_id, job_dir), daemon=True).start()
    return job_id, job_id + '/index.m3u8'

@app.route('/api/hls/<path:file_path>')
def api_hls(file_path):
    full_path = os.path.join(RECORDINGS_PATH, file_path)
    if not os.path.exists(full_path):
        abort(404)
    if not os.path.abspath(full_path).startswith(os.path.abspath(RECORDINGS_PATH)):
        abort(403)
    fast_mode = request.args.get('quality', 'fast') == 'fast'
    # Fast start with event playlist
    playlist_type = 'event'
    job_and_subpath = start_hls_vod(full_path, fast_mode, playlist_type)
    if not job_and_subpath:
        return jsonify({'error': 'HLS queue full'}), 503
    job_id, subpath = job_and_subpath
    # Briefly wait for playlist file to appear to avoid immediate 404s when the player requests it
    playlist_fs_path = os.path.join(HLS_PATH, subpath)
    # Wait briefly for initial playlist to appear; do not wait for ENDLIST
    for _ in range(200):  # up to ~10s
        if os.path.exists(playlist_fs_path):
            break
        time.sleep(0.05)

    # Probe accurate duration and include it in the response
    duration_sec = None
    try:
        result = subprocess.run([
            'ffprobe', '-v', 'error', '-select_streams', 'v:0',
            '-show_entries', 'format=duration', '-of', 'default=nk=1:nw=1', full_path
        ], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            duration_str = (result.stdout or '').strip()
            if duration_str:
                duration_sec = float(duration_str)
    except Exception:
        duration_sec = None

    return jsonify({'playlist': f'/api/files/{subpath}', 'duration': duration_sec, 'job': job_id})

@app.route('/api/hls/<job_id>', methods=['DELETE'])
def stop_hls_job(job_id):
    """Terminate an active HLS job and remove its temporary files."""
    with hls_jobs_lock:
        job = hls_jobs.pop(job_id, None)
    if not job:
        # Nothing to do
        return ('', 204)
    proc = job.get('proc')
    job_dir = job.get('dir')
    # Try to stop process
    try:
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill(); proc.wait()
    except Exception:
        pass
    # Remove directory
    try:
        if job_dir and os.path.isdir(job_dir):
            shutil.rmtree(job_dir, ignore_errors=True)
    except Exception:
        pass
    return ('', 204)

@app.route('/api/files/<path:file_path>')
def serve_file(file_path):
    """Serve video and image files with proper range request support"""
    full_path = os.path.join(RECORDINGS_PATH, file_path)
    hls_path = os.path.join(HLS_PATH, file_path)

    # FIRST: Serve HLS assets under HLS_PATH (wait for segments if needed)
    safe_hls_root = os.path.abspath(HLS_PATH)
    safe_hls_path = os.path.abspath(hls_path)
    if safe_hls_path.startswith(safe_hls_root):
        ext = file_path.lower().split('.')[-1]
        if ext == 'm3u8':
            if not os.path.exists(safe_hls_path):
                abort(404)
            resp = send_file(safe_hls_path, mimetype='application/vnd.apple.mpegurl')
            resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            resp.headers['Pragma'] = 'no-cache'
            resp.headers['Expires'] = '0'
            return resp
        if ext in ('m4s', 'mp4', 'ts'):
            # Block until segment exists (for VOD playlist published ahead of segments)
            for _ in range(1200):  # up to ~60s
                if os.path.exists(safe_hls_path):
                    break
                time.sleep(0.05)
            if not os.path.exists(safe_hls_path):
                abort(404)
            if ext == 'm4s':
                return send_file(safe_hls_path, mimetype='video/iso.segment')
            if ext == 'mp4':  # e.g., init.mp4
                return send_file(safe_hls_path, mimetype='video/mp4')
            if ext == 'ts':
                return send_file(safe_hls_path, mimetype='video/mp2t')
            return send_file(safe_hls_path)
    
    if not os.path.exists(full_path):
        abort(404)
    
    if not os.path.abspath(full_path).startswith(os.path.abspath(RECORDINGS_PATH)):
        abort(403)  # Prevent directory traversal
    
    # (No duplicate secondary HLS serve block)

    # Determine MIME type based on file extension
    file_extension = file_path.lower().split('.')[-1]
    
    if file_extension == 'm3u8':
        full_hls_path = os.path.join(HLS_PATH, file_path)
        if not os.path.exists(full_hls_path):
            abort(404)
        resp = send_file(full_hls_path, mimetype='application/vnd.apple.mpegurl')
        resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        resp.headers['Pragma'] = 'no-cache'
        resp.headers['Expires'] = '0'
        return resp
    if file_extension == 'mp4':
        # Serve original MP4 with Range support (no ffmpeg streaming here)
        file_size = os.path.getsize(full_path)
        byte_start, byte_end = get_range_requests(file_size)
        if byte_start is None:
            return send_file(full_path, mimetype='video/mp4')
        def generate_original():
            with open(full_path, 'rb') as f:
                f.seek(byte_start)
                remaining = byte_end - byte_start + 1
                while remaining:
                    chunk_size = min(8192, remaining)
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    remaining -= len(chunk)
                    yield chunk
        response = Response(
            generate_original(),
            206,
            mimetype='video/mp4',
            direct_passthrough=True
        )
        response.headers.add('Accept-Ranges', 'bytes')
        response.headers.add('Content-Range', f'bytes {byte_start}-{byte_end}/{file_size}')
        response.headers.add('Content-Length', str(byte_end - byte_start + 1))
        return response
    if file_extension in ['m4s', 'ts']:
        # Non-HLS-path segments requested directly (unlikely); just serve file
        return send_file(full_path)
    elif file_extension in ['jpg', 'jpeg']:
        return send_file(full_path, mimetype='image/jpeg')
    elif file_extension == 'png':
        return send_file(full_path, mimetype='image/png')
    else:
        return send_file(full_path)

@app.route('/video-info/<path:file_path>')
def video_info(file_path):
    """Get basic info about a video file"""
    full_path = os.path.join(RECORDINGS_PATH, file_path)
    
    if not os.path.exists(full_path):
        return jsonify({'error': 'File not found'}), 404
    
    if not full_path.lower().endswith('.mp4'):
        return jsonify({'error': 'Not an MP4 file'}), 400
    
    try:
        file_size = os.path.getsize(full_path)
        
        # Read first few bytes to check MP4 header
        with open(full_path, 'rb') as f:
            header = f.read(32)
        
        # Basic MP4 validation - should start with ftyp box
        is_valid_mp4 = len(header) >= 8 and header[4:8] == b'ftyp'
        
        return jsonify({
            'file_path': file_path,
            'file_size': file_size,
            'header_hex': header.hex(),
            'is_valid_mp4': is_valid_mp4,
            'first_4_bytes': header[:4].hex() if len(header) >= 4 else None,
            'ftyp_check': header[4:8] == b'ftyp' if len(header) >= 8 else False
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/transcode-status')
def transcode_status():
    """Get current transcoding status"""
    with transcode_lock:
        return jsonify({
            'active_transcodes': active_transcodes,
            'max_concurrent': MAX_CONCURRENT_TRANSCODES,
            'available_slots': MAX_CONCURRENT_TRANSCODES - active_transcodes
        })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
