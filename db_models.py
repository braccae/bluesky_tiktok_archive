#!/usr/bin/env python3

import sqlite3
import os
from contextlib import contextmanager
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tiktok_data.db')

def get_db_connection():
    """Create a connection to the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # This enables column access by name
    return conn

@contextmanager
def db_connection():
    """Context manager for database connections."""
    conn = get_db_connection()
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    """Initialize the database with required tables."""
    with db_connection() as conn:
        cursor = conn.cursor()
        
        # Create authors table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS authors (
            id TEXT PRIMARY KEY,
            uniqueIds TEXT,
            nicknames TEXT,
            followerCount INTEGER,
            heartCount INTEGER,
            videoCount INTEGER
        )
        ''')
        
        # Create videos table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            id TEXT PRIMARY KEY,
            authorId TEXT,
            createTime INTEGER,
            diggCount INTEGER,
            playCount INTEGER,
            audioId TEXT,
            size TEXT,
            description TEXT,
            uploaded INTEGER DEFAULT 0,
            uploadDate TEXT,
            lengthSeconds REAL,
            FOREIGN KEY (authorId) REFERENCES authors (id)
        )
        ''')
        
        # Create liked videos table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS liked_videos (
            videoId TEXT PRIMARY KEY,
            FOREIGN KEY (videoId) REFERENCES videos (id)
        )
        ''')
        
        # Create bookmarked videos table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS bookmarked_videos (
            videoId TEXT PRIMARY KEY,
            FOREIGN KEY (videoId) REFERENCES videos (id)
        )
        ''')
        
        # Create following table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS following (
            authorId TEXT PRIMARY KEY,
            FOREIGN KEY (authorId) REFERENCES authors (id)
        )
        ''')
        
        # Create user table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS user (
            id TEXT PRIMARY KEY,
            uniqueId TEXT,
            nickname TEXT
        )
        ''')

        # Create metadata table for schema version and import timestamps
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        ''')
        
        conn.commit()

def mark_video_uploaded(video_id):
    """Mark a video as uploaded in the database."""
    with db_connection() as conn:
        cursor = conn.cursor()
        now = datetime.now().isoformat()
        cursor.execute(
            "UPDATE videos SET uploaded = 1, uploadDate = ? WHERE id = ?",
            (now, video_id)
        )
        conn.commit()
        return cursor.rowcount > 0

def get_next_unuploaded_video(source_type, author_id=None):
    """Get the next unuploaded video of the specified type.
    
    Args:
        source_type: One of 'created', 'liked', or 'bookmarked'
        author_id: Optional author ID to filter by (for 'created' videos)
        
    Returns:
        Row object with video data or None if no videos found
    """
    # Import here to avoid circular imports
    import os
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Get max video length from environment
    max_length = os.getenv("MAX_VIDEO_LENGTH")
    max_length_filter = ""
    
    # Parse max length, checking for valid value
    if max_length:
        try:
            max_length = float(max_length)
            if max_length > 0:
                # Only apply length filter if a positive value is set
                max_length_filter = " AND (lengthSeconds IS NULL OR lengthSeconds <= ?)"
        except (ValueError, TypeError):
            # If parsing fails, don't apply length filter
            max_length = 0
    else:
        # If not set, don't apply length filter
        max_length = 0
    
    with db_connection() as conn:
        cursor = conn.cursor()
        
        if source_type == 'liked':
            query = f'''
            SELECT v.* FROM videos v
            JOIN liked_videos l ON v.id = l.videoId
            WHERE v.uploaded = 0{max_length_filter}
            ORDER BY v.createTime ASC
            LIMIT 1
            '''
        elif source_type == 'bookmarked':
            query = f'''
            SELECT v.* FROM videos v
            JOIN bookmarked_videos b ON v.id = b.videoId
            WHERE v.uploaded = 0{max_length_filter}
            ORDER BY v.createTime ASC
            LIMIT 1
            '''
        elif source_type == 'created':
            if author_id:
                query = f'''
                SELECT * FROM videos
                WHERE authorId = ? AND uploaded = 0{max_length_filter}
                ORDER BY createTime ASC
                LIMIT 1
                '''
            else:
                # Get videos by the user (using data from user table)
                query = f'''
                SELECT v.* FROM videos v
                JOIN user u
                WHERE v.authorId = u.id AND v.uploaded = 0{max_length_filter}
                ORDER BY v.createTime ASC
                LIMIT 1
                '''
        else:
            raise ValueError(f"Invalid source_type: {source_type}")
        
        # Execute query with or without max_length parameter
        if max_length > 0:
            if source_type == 'created' and author_id:
                cursor.execute(query, (author_id, max_length))
            elif max_length_filter:
                cursor.execute(query, (max_length,))
            else:
                cursor.execute(query)
        else:
            if source_type == 'created' and author_id:
                cursor.execute(query, (author_id,))
            else:
                cursor.execute(query)
        
        return cursor.fetchone()

def get_author(author_id):
    """Get author data by ID."""
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM authors WHERE id = ?", (author_id,))
        return cursor.fetchone()

def get_video_file_path(tiktok_dir, video_id, get_length=False):
    """Get the file path for a video in the TikTok archive.
    
    Args:
        tiktok_dir: Path to the TikTok archive directory
        video_id: ID of the video to find
        get_length: If True, also return the video length in seconds
        
    Returns:
        If get_length is False: path to the video file or None if not found
        If get_length is True: (path, length_seconds) tuple or (None, None) if not found
    """
    # Common locations based on TikTok archive structure
    possible_locations = [
        os.path.join(tiktok_dir, 'data', 'videos', f"{video_id}.mp4"),
        os.path.join(tiktok_dir, 'videos', f"{video_id}.mp4"),
    ]
    
    for location in possible_locations:
        if os.path.exists(location):
            if get_length:
                length = get_video_length(location)
                return location, length
            return location
    
    # If video not found in expected locations, search recursively
    for root, _, files in os.walk(tiktok_dir):
        for file in files:
            if file == f"{video_id}.mp4":
                path = os.path.join(root, file)
                if get_length:
                    length = get_video_length(path)
                    return path, length
                return path
    
    if get_length:
        return None, None
    return None

def get_video_length(video_path):
    """Get the length of a video in seconds using OpenCV."""
    try:
        import cv2
        video = cv2.VideoCapture(video_path)
        
        # Get frame rate and frame count to calculate duration
        if not video.isOpened():
            return None
            
        frame_count = int(video.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = video.get(cv2.CAP_PROP_FPS)
        
        # Calculate duration in seconds
        duration = frame_count / fps if fps > 0 else 0
        
        # Release the video capture object
        video.release()
        
        return duration
    except Exception as e:
        print(f"Error getting video length for {video_path}: {e}")
        return None

# Call init_db if this script is run directly
if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
