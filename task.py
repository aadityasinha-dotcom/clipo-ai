from celery import Celery
from time import sleep
import subprocess
import os
from database import update_video_metadata, get_video_by_id
from bson import ObjectId

celery_app = Celery(
    "worker",
    backend="redis://localhost:6379/0",
    broker="redis://localhost:6379/0"
)

@celery_app.task
def run_long_task(iterations):
    result = 0
    for i in range(iterations):
        result += i
        sleep(2)
    return result

@celery_app.task
def process_video_task(video_id, file_path):
    try:
        # Update status to processing
        update_video_metadata(video_id, {"status": "processing"})
        
        # Extract video duration using ffmpeg
        duration = get_video_duration(file_path)
        
        # Generate thumbnail at 10% of video duration
        thumbnail_path = generate_thumbnail(file_path, duration, video_id)
        
        # Create thumbnail URL
        thumbnail_url = f"http://localhost:8000/thumbnails/{os.path.basename(thumbnail_path)}"
        
        # Update MongoDB with results
        update_data = {
            "status": "done",
            "duration": duration,
            "thumbnail_url": thumbnail_url
        }
        update_video_metadata(video_id, update_data)
        
        return {
            "video_id": video_id,
            "status": "completed",
            "duration": duration,
            "thumbnail_url": thumbnail_url
        }
        
    except Exception as e:
        # Update status to error
        update_video_metadata(video_id, {
            "status": "error",
            "error_message": str(e)
        })
        raise e

def get_video_duration(file_path):
    """Extract video duration using ffmpeg"""
    try:
        cmd = [
            'ffmpeg', '-i', file_path, '-f', 'null', '-',
            '-v', 'quiet', '-show_entries', 'format=duration',
            '-of', 'csv=p=0'
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # Alternative method using ffprobe (more reliable)
        cmd = [
            'ffprobe', '-v', 'quiet', '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1', file_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        duration_seconds = float(result.stdout.strip())
        
        # Convert to HH:MM:SS format
        hours = int(duration_seconds // 3600)
        minutes = int((duration_seconds % 3600) // 60)
        seconds = int(duration_seconds % 60)
        
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
    except subprocess.CalledProcessError as e:
        raise Exception(f"Failed to extract video duration: {e}")
    except Exception as e:
        raise Exception(f"Error processing video duration: {e}")

def generate_thumbnail(file_path, duration_str, video_id):
    """Generate thumbnail at 10% of video duration"""
    try:
        # Convert duration back to seconds for calculation
        time_parts = duration_str.split(':')
        total_seconds = int(time_parts[0]) * 3600 + int(time_parts[1]) * 60 + int(time_parts[2])
        
        # Calculate 10% timestamp
        thumbnail_time = total_seconds * 0.1
        
        # Format timestamp for ffmpeg
        thumb_hours = int(thumbnail_time // 3600)
        thumb_minutes = int((thumbnail_time % 3600) // 60)
        thumb_seconds = int(thumbnail_time % 60)
        timestamp = f"{thumb_hours:02d}:{thumb_minutes:02d}:{thumb_seconds:02d}"
        
        # Generate thumbnail filename
        video_filename = os.path.splitext(os.path.basename(file_path))[0]
        thumbnail_filename = f"{video_id}_{video_filename}.jpg"
        thumbnail_path = f"thumbnails/{thumbnail_filename}"
        
        # Use ffmpeg to generate thumbnail
        cmd = [
            'ffmpeg', '-i', file_path, '-ss', timestamp,
            '-vframes', '1', '-q:v', '2', '-y', thumbnail_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        return thumbnail_path
        
    except subprocess.CalledProcessError as e:
        raise Exception(f"Failed to generate thumbnail: {e}")
    except Exception as e:
        raise Exception(f"Error generating thumbnail: {e}")

def get_task_status(task_id):
    task_result = celery_app.AsyncResult(task_id)
    if task_result.ready():
        if task_result.successful():
            return {
                "ready": task_result.ready(),
                "successful": task_result.successful(),
                "value": task_result.result,
            }
        else:
            return {"status": "ERROR", "error_message": str(task_result.result)}
    else:
        return {"status": "Running"}
