from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client["video_processor"]
videos_collection = db["videos"]

def insert_video_metadata(video_data):
    """Insert video metadata into MongoDB"""
    try:
        result = videos_collection.insert_one(video_data)
        return result.inserted_id
    except Exception as e:
        raise Exception(f"Failed to insert video metadata: {e}")

def get_video_by_id(video_id):
    """Get video metadata by ID"""
    try:
        video = videos_collection.find_one({"_id": ObjectId(video_id)})
        return video
    except Exception as e:
        raise Exception(f"Failed to get video by ID: {e}")

def update_video_metadata(video_id, update_data):
    """Update video metadata in MongoDB"""
    try:
        result = videos_collection.update_one(
            {"_id": ObjectId(video_id)},
            {"$set": update_data}
        )
        return result.modified_count > 0
    except Exception as e:
        raise Exception(f"Failed to update video metadata: {e}")

def get_all_videos():
    """Get all videos from MongoDB"""
    try:
        videos = list(videos_collection.find())
        return videos
    except Exception as e:
        raise Exception(f"Failed to get all videos: {e}")

def delete_video_by_id(video_id):
    """Delete video metadata by ID"""
    try:
        result = videos_collection.delete_one({"_id": ObjectId(video_id)})
        return result.deleted_count > 0
    except Exception as e:
        raise Exception(f"Failed to delete video: {e}")
