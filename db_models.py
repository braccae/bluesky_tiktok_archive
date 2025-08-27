#!/usr/bin/env python3

import os
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DB_TYPE = os.getenv('DB_TYPE', 'sqlite')
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tiktok_data.db')

def _get_db_type():
    """Returns the configured database type."""
    return DB_TYPE

def get_db_connection():
    """Create a connection to the configured database."""
    if _get_db_type() == 'postgres':
        import psycopg2
        import psycopg2.extras
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT'),
                dbname=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                cursor_factory=psycopg2.extras.DictCursor
            )
            return conn
        except psycopg2.OperationalError as e:
            print(f"Error connecting to PostgreSQL: {e}")
            # Exit gracefully if we can't connect to the database
            sys.exit(1)
    else:
        # Default to SQLite
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
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
        is_postgres = _get_db_type() == 'postgres'

        # Table creation queries
        queries = {
            "authors": """
                CREATE TABLE IF NOT EXISTS authors (
                    id TEXT PRIMARY KEY,
                    uniqueIds JSONB,
                    nicknames JSONB,
                    followerCount BIGINT,
                    heartCount BIGINT,
                    videoCount BIGINT
                )""",
            "videos": """
                CREATE TABLE IF NOT EXISTS videos (
                    id TEXT PRIMARY KEY,
                    authorId TEXT,
                    createTime BIGINT,
                    diggCount BIGINT,
                    playCount BIGINT,
                    audioId TEXT,
                    size TEXT,
                    description TEXT,
                    uploaded BOOLEAN DEFAULT FALSE,
                    uploadDate TIMESTAMP,
                    lengthSeconds REAL,
                    filePath TEXT,
                    is_liked BOOLEAN DEFAULT FALSE,
                    is_bookmarked BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY (authorId) REFERENCES authors (id)
                )""",
            "following": """
                CREATE TABLE IF NOT EXISTS following (
                    authorId TEXT PRIMARY KEY,
                    FOREIGN KEY (authorId) REFERENCES authors (id) ON DELETE CASCADE
                )""",
            "user": """
                CREATE TABLE IF NOT EXISTS "user" (
                    id TEXT PRIMARY KEY,
                    uniqueId TEXT,
                    nickname TEXT
                )""",
            "metadata": """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )"""
        }

        # Adjust queries for SQLite if needed
        if not is_postgres:
            queries["authors"] = queries["authors"].replace("JSONB", "TEXT")
            queries["videos"] = queries["videos"].replace("BOOLEAN", "INTEGER").replace("TIMESTAMP", "TEXT")
            queries["user"] = queries["user"].replace('"user"', 'user')

        for table, query in queries.items():
            cursor.execute(query)

        conn.commit()

def _get_placeholder():
    """Returns the correct placeholder string for the configured database."""
    return '%s' if _get_db_type() == 'postgres' else '?'

def mark_video_uploaded(video_id):
    """Mark a video as uploaded in the database."""
    with db_connection() as conn:
        cursor = conn.cursor()
        now = datetime.now()
        placeholder = _get_placeholder()
        
        query = f"UPDATE videos SET uploaded = TRUE, uploadDate = {placeholder} WHERE id = {placeholder}"
        if _get_db_type() != 'postgres':
            query = f"UPDATE videos SET uploaded = 1, uploadDate = {placeholder} WHERE id = {placeholder}"

        cursor.execute(query, (now, video_id))
        conn.commit()
        return cursor.rowcount > 0

def get_next_unuploaded_video(source_type, author_id=None):
    """Get the next unuploaded video of the specified type."""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    max_length = os.getenv("MAX_VIDEO_LENGTH")
    max_length_filter = ""
    params = []

    if max_length:
        try:
            max_length = float(max_length)
            if max_length > 0:
                placeholder = _get_placeholder()
                max_length_filter = f"(lengthSeconds IS NULL OR lengthSeconds <= {placeholder})"
                params.append(max_length)
        except (ValueError, TypeError):
            max_length = 0
    else:
        max_length = 0

    with db_connection() as conn:
        cursor = conn.cursor()
        is_postgres = _get_db_type() == 'postgres'
        true_value = 'true' if is_postgres else '1'
        
        where_clauses = []
        
        if source_type == 'liked':
            where_clauses.append(f"is_liked = {true_value}")
        elif source_type == 'bookmarked':
            where_clauses.append(f"is_bookmarked = {true_value}")
        elif source_type == 'created':
            if author_id:
                placeholder = _get_placeholder()
                where_clauses.append(f"authorId = {placeholder}")
                params.insert(0, author_id)
            else:
                user_table = '"user"' if is_postgres else 'user'
                where_clauses.append(f"authorId IN (SELECT id FROM {user_table})")
        else:
            raise ValueError(f"Invalid source_type: {source_type}")

        uploaded_check = "uploaded = false" if is_postgres else "uploaded = 0"
        where_clauses.append(uploaded_check)

        if max_length_filter:
            where_clauses.append(max_length_filter)

        query = f"SELECT * FROM videos WHERE {' AND '.join(where_clauses)} ORDER BY createTime ASC LIMIT 1"
        
        cursor.execute(query, tuple(params))
        return cursor.fetchone()

def get_author(author_id):
    """Get author data by ID."""
    with db_connection() as conn:
        cursor = conn.cursor()
        placeholder = _get_placeholder()
        query = f"SELECT * FROM authors WHERE id = {placeholder}"
        cursor.execute(query, (author_id,))
        return cursor.fetchone()

def get_video_file_path(tiktok_dir, video_id, get_length=False, verbose=False):
    """Get the file path for a video in the TikTok archive."""
    possible_locations = [
        os.path.join(tiktok_dir, 'data', 'videos', f"{video_id}.mp4"),
        os.path.join(tiktok_dir, 'videos', f"{video_id}.mp4"),
        os.path.join(tiktok_dir, 'data', 'Likes', 'videos', f"{video_id}.mp4"),
        os.path.join(tiktok_dir, 'data', 'Favorites', 'videos', f"{video_id}.mp4"),
        os.path.join(tiktok_dir, 'data', 'Followed', 'videos', f"{video_id}.mp4"),
    ]
    
    if verbose:
        print(f"    Searching for video {video_id} in common locations...")

    for location in possible_locations:
        if verbose:
            print(f"      Checking: {location}")
        if os.path.exists(location):
            if verbose:
                print(f"    Found video at: {location}")
            if get_length:
                length = get_video_length(location, verbose=verbose)
                return location, length
            return location
    
    if verbose:
        print(f"    Video {video_id} not in common locations, searching recursively...")

    for root, _, files in os.walk(tiktok_dir):
        for file in files:
            if file == f"{video_id}.mp4":
                path = os.path.join(root, file)
                if verbose:
                    print(f"    Found video at: {path}")
                if get_length:
                    length = get_video_length(path, verbose=verbose)
                    return path, length
                return path
    
    if get_length:
        return None, None
    return None

def get_video_length(video_path, verbose=False):
    """Get the length of a video in seconds using ffmpeg."""
    import ffmpeg
    try:
        if verbose:
            print(f"      Getting length for {video_path} using ffmpeg...")
        probe = ffmpeg.probe(video_path)
        length = float(probe['format']['duration'])
        if verbose:
            print(f"      Length is: {length:.2f}s")
        return length
    except Exception as e:
        print(f"    Error getting video length for {video_path}: {e}")
        return None

if __name__ == "__main__":
    init_db()
    if _get_db_type() == 'sqlite':
        print(f"Database initialized at {DB_PATH}")
    else:
        print("PostgreSQL database initialized.")