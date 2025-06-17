# celery_worker.py
from celery import Celery
from pymongo import MongoClient
from bson import ObjectId
import subprocess
import os
import json
from datetime import datetime, timedelta

# Celery configuration with explicit Redis transport
celery_app = Celery('video_processor')

# Configure Celery with explicit settings
celery_app.conf.update(
    broker_url='redis://localhost:6379/0',
    result_backend='redis://localhost:6379/0',
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    broker_connection_retry_on_startup=True,
    broker_connection_retry=True,
    broker_connection_max_retries=10,
)

# MongoDB configuration
MONGO_URL = "mongodb://localhost:27017"
DATABASE_NAME = "video_processing"
COLLECTION_NAME = "videos"

# Initialize MongoDB client
client = MongoClient(MONGO_URL)
db = client[DATABASE_NAME]
videos_collection = db[COLLECTION_NAME]

# Directories
THUMBNAIL_DIR = "thumbnails"
os.makedirs(THUMBNAIL_DIR, exist_ok=True)

def get_video_duration(video_path):
    """
    Extract video duration using FFmpeg
    """
    try:
        cmd = [
            'ffprobe', '-v', 'quiet', '-print_format', 'json',
            '-show_format', '-show_streams', video_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        
        # Get duration from format or streams
        duration = None
        if 'format' in data and 'duration' in data['format']:
            duration = float(data['format']['duration'])
        else:
            # Try to get duration from video stream
            for stream in data.get('streams', []):
                if stream.get('codec_type') == 'video' and 'duration' in stream:
                    duration = float(stream['duration'])
                    break
        
        if duration:
            # Convert to HH:MM:SS format
            hours = int(duration // 3600)
            minutes = int((duration % 3600) // 60)
            seconds = int(duration % 60)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}", duration
        
        return None, None
        
    except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError) as e:
        print(f"Error extracting duration: {e}")
        return None, None

def generate_thumbnail(video_path, output_path, timestamp_seconds):
    """
    Generate thumbnail at specified timestamp using FFmpeg
    """
    try:
        cmd = [
            'ffmpeg', '-i', video_path, '-ss', str(timestamp_seconds),
            '-vframes', '1', '-vf', 'scale=320:240', '-y', output_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"Error generating thumbnail: {e}")
        return False

@celery_app.task(bind=True)
def process_video_task(self, video_id):
    """
    Background task to process video: extract duration and generate thumbnail
    """
    try:
        # Update status to processing
        videos_collection.update_one(
            {"_id": ObjectId(video_id)},
            {"$set": {"status": "processing", "processing_started": datetime.now()}}
        )
        
        # Get video document
        video = videos_collection.find_one({"_id": ObjectId(video_id)})
        if not video:
            raise Exception("Video not found in database")
        
        video_path = video["file_path"]
        if not os.path.exists(video_path):
            raise Exception("Video file not found on disk")
        
        # Extract video duration
        duration_str, duration_seconds = get_video_duration(video_path)
        if not duration_str:
            raise Exception("Could not extract video duration")
        
        # Generate thumbnail at 10% of video duration
        thumbnail_timestamp = duration_seconds * 0.1
        thumbnail_filename = f"{video_id}_thumbnail.jpg"
        thumbnail_path = os.path.join(THUMBNAIL_DIR, thumbnail_filename)
        
        if not generate_thumbnail(video_path, thumbnail_path, thumbnail_timestamp):
            raise Exception("Could not generate thumbnail")
        
        # Generate thumbnail URL (assuming the API runs on localhost:8000)
        thumbnail_url = f"http://localhost:8000/thumbnails/{thumbnail_filename}"
        
        # Update MongoDB with results
        update_data = {
            "status": "done",
            "duration": duration_str,
            "duration_seconds": duration_seconds,
            "thumbnail_url": thumbnail_url,
            "thumbnail_path": thumbnail_path,
            "thumbnail_filename": thumbnail_filename,
            "processing_completed": datetime.now()
        }
        
        videos_collection.update_one(
            {"_id": ObjectId(video_id)},
            {"$set": update_data}
        )
        
        return {
            "status": "success",
            "video_id": video_id,
            "duration": duration_str,
            "thumbnail_url": thumbnail_url
        }
        
    except Exception as e:
        error_message = str(e)
        print(f"Error processing video {video_id}: {error_message}")
        
        # Update status to error
        videos_collection.update_one(
            {"_id": ObjectId(video_id)},
            {"$set": {
                "status": "error",
                "error_message": error_message,
                "processing_failed": datetime.now()
            }}
        )
        
        # Re-raise the exception so Celery knows the task failed
        raise self.retry(exc=e, countdown=60, max_retries=3)

@celery_app.task
def cleanup_old_files():
    """
    Periodic task to clean up old video files (optional)
    """
    try:
        # Find videos older than 30 days with status "done" or "error"
        cutoff_date = datetime.now() - timedelta(days=30)
        old_videos = videos_collection.find({
            "upload_time": {"$lt": cutoff_date},
            "status": {"$in": ["done", "error"]}
        })
        
        for video in old_videos:
            # Remove files
            if os.path.exists(video["file_path"]):
                os.remove(video["file_path"])
            
            if "thumbnail_path" in video and os.path.exists(video["thumbnail_path"]):
                os.remove(video["thumbnail_path"])
            
            # Remove from database
            videos_collection.delete_one({"_id": video["_id"]})
        
        return "Cleanup completed"
        
    except Exception as e:
        print(f"Error during cleanup: {e}")
        return f"Cleanup failed: {e}"

# Configure periodic tasks
celery_app.conf.beat_schedule = {
    'cleanup-old-files': {
        'task': 'celery_worker.cleanup_old_files',
        'schedule': 24 * 60 * 60.0,  # Run daily
    },
}

if __name__ == '__main__':
    celery_app.start()
