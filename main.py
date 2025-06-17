from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import os
from datetime import datetime
from task import process_video_task, get_task_status
from database import get_video_by_id, insert_video_metadata
from bson import ObjectId
import shutil

app = FastAPI()

# Create directories if they don't exist
os.makedirs("uploads", exist_ok=True)
os.makedirs("thumbnails", exist_ok=True)

# Mount static files for serving thumbnails
app.mount("/thumbnails", StaticFiles(directory="thumbnails"), name="thumbnails")

@app.get("/")
def read_root():
    return {"message": "Hello, FastAPI Video Processor!"}

@app.post("/upload-video/")
async def upload_video(file: UploadFile = File(...)):
    # Enhanced file validation
    print(f"Uploaded file: {file.filename}")
    print(f"Content type: {file.content_type}")
    print(f"File size: {file.size if hasattr(file, 'size') else 'Unknown'}")
    
    # List of accepted video extensions and MIME types
    video_extensions = ['.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv', '.webm', '.m4v']
    video_mime_types = [
        'video/mp4', 'video/avi', 'video/quicktime', 'video/x-msvideo',
        'video/x-ms-wmv', 'video/x-flv', 'video/webm', 'video/x-matroska',
        'application/octet-stream'  # Sometimes videos are detected as this
    ]
    
    # Check file extension
    file_extension = os.path.splitext(file.filename.lower())[1] if file.filename else ""
    
    # Validate file type by extension or MIME type
    is_valid_video = (
        file.content_type in video_mime_types or
        file.content_type.startswith('video/') or
        file_extension in video_extensions
    )
    
    if not is_valid_video:
        raise HTTPException(
            status_code=400, 
            detail=f"File must be a video. Received: {file.content_type}, Extension: {file_extension}"
        )
    
    # Save file locally
    file_location = f"uploads/{file.filename}"
    with open(file_location, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Create metadata document
    video_metadata = {
        "filename": file.filename,
        "upload_time": datetime.utcnow().isoformat(),
        "status": "pending",
        "file_path": file_location
    }
    
    # Insert into MongoDB
    video_id = insert_video_metadata(video_metadata)
    
    # Start Celery background task
    task = process_video_task.delay(str(video_id), file_location)
    
    return {
        "video_id": str(video_id),
        "task_id": task.id,
        "message": "Video uploaded successfully, processing started"
    }

@app.get("/video-status/{video_id}")
def get_video_status(video_id: str):
    try:
        # Get video from MongoDB
        video = get_video_by_id(video_id)
        if not video:
            raise HTTPException(status_code=404, detail="Video not found")
        
        return {
            "id": str(video["_id"]),
            "status": video["status"]
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/video-metadata/{video_id}")
def get_video_metadata(video_id: str):
    try:
        # Get video from MongoDB
        video = get_video_by_id(video_id)
        if not video:
            raise HTTPException(status_code=404, detail="Video not found")
        
        # Convert ObjectId to string for JSON serialization
        video["_id"] = str(video["_id"])
        
        return video
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/task/{task_id}")
def check_task(task_id: str):
    status = get_task_status(task_id)
    return {"task_id": task_id, "status": status}
