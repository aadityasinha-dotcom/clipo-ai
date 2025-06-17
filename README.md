## Installation

## Install Python dependencies:

`pip install -r requirements.txt`

#### Start services:

1. Start MongoDB
``sudo systemctl start mongod  # Linux
brew services start mongodb-community  # macOS``

2. Start Redis
``sudo systemctl start redis-server  # Linux
brew services start redis  # macOS``

3. Start Celery worker:

`celery -A task.celery_app worker --loglevel=info`

4. Start FastAPI server:

`uvicorn main:app --reload --host 0.0.0.0 --port 8000`

## API Usage Examples
1. Upload Video

``curl -X POST "http://localhost:8000/upload-video/" \
     -H "accept: application/json" \
     -H "Content-Type: multipart/form-data" \
     -F "file=@your_video.mp4"``

2. Check Video Status

`curl -X GET "http://localhost:8000/video-status/648b5f8c9d7e2a1b3c4d5e6f"`

3. Get Full Video Metadata

`curl -X GET "http://localhost:8000/video-metadata/648b5f8c9d7e2a1b3c4d5e6f"`

