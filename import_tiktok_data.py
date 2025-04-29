#!/usr/bin/env python3

import json
import os
import sys
import sqlite3
import argparse
from pathlib import Path
from dotenv import load_dotenv
from db_models import db_connection, init_db, DB_PATH

def load_tiktok_data(facts_json_path):
    """Load TikTok data from facts.json file."""
    try:
        with open(facts_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"Error loading TikTok data: {e}")
        sys.exit(1)

def import_authors(conn, authors_data):
    """Import authors into the database."""
    cursor = conn.cursor()
    authors_imported = 0
    
    if args.verbose:
        print(f"\nProcessing {len(authors_data)} authors...")
    
    for author_id, author in authors_data.items():
        if author_id and author:
            # Convert lists to JSON strings for storage
            uniqueIds = json.dumps(author.get('uniqueIds', []))
            nicknames = json.dumps(author.get('nicknames', []))
            
            if args.verbose:
                unique_id = author.get('uniqueIds', [])[0] if author.get('uniqueIds') else "Unknown"
                nickname = author.get('nicknames', [])[0] if author.get('nicknames') else unique_id
                print(f"  Importing author: @{unique_id} ({nickname})")
            
            cursor.execute(
                """
                INSERT OR REPLACE INTO authors 
                (id, uniqueIds, nicknames, followerCount, heartCount, videoCount)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    author_id,
                    uniqueIds,
                    nicknames,
                    author.get('followerCount', 0),
                    author.get('heartCount', 0),
                    author.get('videoCount', 0)
                )
            )
            authors_imported += 1
            
            # Print progress every 100 authors if verbose
            if args.verbose and authors_imported % 100 == 0:
                print(f"  Imported {authors_imported} authors so far...")
    
    conn.commit()
    return authors_imported

def import_videos(conn, videos_data, descriptions_data):
    """Import videos into the database."""
    from db_models import get_video_file_path
    
    cursor = conn.cursor()
    videos_imported = 0
    videos_with_length = 0
    
    if args.verbose:
        print(f"\nProcessing {len(videos_data)} videos...")
    
    # Get TikTok directory path from .env file
    tiktok_dir = os.getenv('TIKTOK_DIR', '')
    if not tiktok_dir:
        print("Warning: TIKTOK_DIR not set in .env file, video lengths will not be calculated")
    
    for video_id, video in videos_data.items():
        if video_id and video:
            # Get description from the descriptions data
            description = descriptions_data.get(video_id, '')
            
            # Format creation time for verbose output
            if args.verbose:
                create_time = video.get('createTime', 0)
                if create_time:
                    from datetime import datetime
                    date_str = datetime.fromtimestamp(create_time/1000).strftime('%Y-%m-%d')
                else:
                    date_str = "Unknown date"
                
                desc_preview = description[:40] + "..." if len(description) > 40 else description
                print(f"  Importing video {video_id} ({date_str}): {desc_preview}")
            
            # Try to get video length if tiktok_dir is set
            length_seconds = None
            stored_path = None
            if tiktok_dir:
                try:
                    video_path, length_seconds = get_video_file_path(tiktok_dir, video_id, get_length=True)
                    if video_path and os.path.exists(video_path):
                        stored_path = video_path
                    else:
                        stored_path = None
                    if length_seconds:
                        if args.verbose:
                            print(f"    Found video file: {video_path}")
                            print(f"    Video length: {int(length_seconds//60)}:{int(length_seconds%60):02d} ({length_seconds:.2f} seconds)")
                        videos_with_length += 1
                    elif args.verbose and video_path:
                        print(f"    Video file found but couldn't determine length: {video_path}")
                except Exception as e:
                    if args.verbose:
                        print(f"    Error getting length for video {video_id}: {e}")
                    stored_path = None
            else:
                stored_path = None
            
            cursor.execute(
                """
                INSERT OR REPLACE INTO videos 
                (id, authorId, createTime, diggCount, playCount, audioId, size, description, uploaded, lengthSeconds, filePath)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                """,
                (
                    video_id,
                    video.get('authorId', ''),
                    video.get('createTime', 0),
                    video.get('diggCount', 0),
                    video.get('playCount', 0),
                    video.get('audioId', ''),
                    video.get('size', ''),
                    description,
                    length_seconds,
                    stored_path
                )
            )
            videos_imported += 1
    
    conn.commit()
    return videos_imported

def import_liked_videos(conn, likes_data):
    """Import liked videos into the database."""
    cursor = conn.cursor()
    liked_imported = 0
    not_found = 0
    
    liked_videos = likes_data.get('officialList', [])
    
    if args.verbose:
        print(f"\nProcessing {len(liked_videos)} liked videos...")
    
    for video_id in liked_videos:
        if video_id:
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO liked_videos (videoId) VALUES (?)",
                    (video_id,)
                )
                if cursor.rowcount > 0:
                    liked_imported += 1
                    if args.verbose and liked_imported % 100 == 0:
                        print(f"  Imported {liked_imported} liked videos so far...")
            except sqlite3.IntegrityError:
                # Video ID doesn't exist in videos table - this is a foreign key constraint
                not_found += 1
                if args.verbose:
                    print(f"  Warning: Liked video {video_id} not found in videos table")
    
    if args.verbose and not_found > 0:
        print(f"  {not_found} liked videos could not be imported due to missing video data")
    
    conn.commit()
    return liked_imported

def import_bookmarked_videos(conn, bookmarked_data):
    """Import bookmarked videos into the database."""
    cursor = conn.cursor()
    bookmarked_imported = 0
    not_found = 0
    
    bookmarked_videos = bookmarked_data.get('officialList', [])
    
    if args.verbose:
        print(f"\nProcessing {len(bookmarked_videos)} bookmarked videos...")
    
    for video_id in bookmarked_videos:
        if video_id:
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO bookmarked_videos (videoId) VALUES (?)",
                    (video_id,)
                )
                if cursor.rowcount > 0:
                    bookmarked_imported += 1
                    if args.verbose and bookmarked_imported % 100 == 0:
                        print(f"  Imported {bookmarked_imported} bookmarked videos so far...")
            except sqlite3.IntegrityError:
                # Video ID doesn't exist in videos table - this is a foreign key constraint
                not_found += 1
                if args.verbose:
                    print(f"  Warning: Bookmarked video {video_id} not found in videos table")
    
    if args.verbose and not_found > 0:
        print(f"  {not_found} bookmarked videos could not be imported due to missing video data")
    
    conn.commit()
    return bookmarked_imported

def import_following(conn, following_data):
    """Import following authors into the database."""
    cursor = conn.cursor()
    following_imported = 0
    not_found = 0
    
    following_authors = following_data.get('officialAuthorList', [])
    
    if args.verbose:
        print(f"\nProcessing {len(following_authors)} followed authors...")
    
    for author_id in following_authors:
        if author_id:
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO following (authorId) VALUES (?)",
                    (author_id,)
                )
                if cursor.rowcount > 0:
                    following_imported += 1
                    if args.verbose and following_imported % 100 == 0:
                        print(f"  Imported {following_imported} followed authors so far...")
                # Get author name for verbose mode
                if args.verbose:
                    cursor.execute("SELECT uniqueIds FROM authors WHERE id = ?", (author_id,))
                    author_info = cursor.fetchone()
                    if author_info and author_info[0]:
                        try:
                            unique_ids = json.loads(author_info[0])
                            if unique_ids:
                                print(f"  Imported followed author: @{unique_ids[0]}")
                        except:
                            pass
            except sqlite3.IntegrityError:
                # Author ID doesn't exist in authors table - this is a foreign key constraint
                not_found += 1
                if args.verbose:
                    print(f"  Warning: Followed author {author_id} not found in authors table")
    
    if args.verbose and not_found > 0:
        print(f"  {not_found} followed authors could not be imported due to missing author data")
    
    conn.commit()
    return following_imported

def import_user_info(conn, user_data):
    """Import user info into the database."""
    if not user_data:
        return False
    
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO user (id, uniqueId, nickname) VALUES (?, ?, ?)",
        (
            user_data.get('id', ''),
            user_data.get('uniqueId', ''),
            user_data.get('nickname', '')
        )
    )
    conn.commit()
    return True

def save_metadata(conn, tiktok_data):
    """Save metadata about the import."""
    cursor = conn.cursor()
    
    # Store schema version
    schema_version = tiktok_data.get('schemaVersion', 0)
    cursor.execute(
        "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
        ('schema_version', str(schema_version))
    )
    
    # Store import timestamp
    import_time = str(int(os.path.getmtime(args.facts_json)) if os.path.exists(args.facts_json) else 0)
    cursor.execute(
        "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
        ('import_timestamp', import_time)
    )
    
    conn.commit()

def main():
    parser = argparse.ArgumentParser(description="Import TikTok data from facts.json into SQLite database.")
    parser.add_argument("--facts-json", help="Path to facts.json file (default is from .env file or [tiktok_dir]/data/.appdata/facts.json).")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show verbose output during import")
    
    global args
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    # Get facts_json path from args, .env, or default
    if not args.facts_json:
        tiktok_dir = os.getenv('TIKTOK_DIR')
        args.facts_json = os.getenv('FACTS_JSON')
        
        if not args.facts_json and tiktok_dir:
            args.facts_json = os.path.join(tiktok_dir, 'data', '.appdata', 'facts.json')
    
    if not args.facts_json or not os.path.exists(args.facts_json):
        print("Error: facts.json file not found. Please specify --facts-json or set TIKTOK_DIR or FACTS_JSON in .env file.")
        sys.exit(1)
    
    print(f"Importing data from {args.facts_json}...")
    
    # Initialize the database
    init_db()
    
    # Show import starting info
    if args.verbose:
        print(f"Starting import from {args.facts_json}")
        print(f"Database: {os.getenv('DB_PATH', DB_PATH)}")
    
    # Load TikTok data
    tiktok_data = load_tiktok_data(args.facts_json)
    
    # Print summary of what we're importing if verbose
    if args.verbose:
        print("\nTikTok archive contains:")
        print(f"  {len(tiktok_data.get('authors', {}))} authors")
        print(f"  {len(tiktok_data.get('videos', {}))} videos")
        print(f"  {len(tiktok_data.get('likes', {}).get('officialList', []))} liked videos")
        print(f"  {len(tiktok_data.get('bookmarked', {}).get('officialList', []))} bookmarked videos")
        print(f"  {len(tiktok_data.get('following', {}).get('officialAuthorList', []))} followed authors")
    
    # Import data into database
    with db_connection() as conn:
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")
        
        if args.verbose:
            print("\n==== Starting database import ====")
        
        # Import authors first (required for foreign key constraints)
        authors_imported = import_authors(conn, tiktok_data.get('authors', {}))
        print(f"Imported {authors_imported} authors")
        
        # Import videos (required for foreign key constraints on liked/bookmarked videos)
        videos_imported = import_videos(
            conn, 
            tiktok_data.get('videos', {}), 
            tiktok_data.get('videoDescriptions', {})
        )
        print(f"Imported {videos_imported} videos")
        
        # Import user info
        user_imported = import_user_info(conn, tiktok_data.get('user', {}))
        if args.verbose and user_imported:
            user_data = tiktok_data.get('user', {})
            print(f"User info imported: {user_data.get('uniqueId', '')} ({user_data.get('nickname', '')})")
        else:
            print(f"User info imported: {user_imported}")
        
        # Import liked videos
        liked_imported = import_liked_videos(conn, tiktok_data.get('likes', {}))
        print(f"Imported {liked_imported} liked videos")
        
        # Import bookmarked videos
        bookmarked_imported = import_bookmarked_videos(conn, tiktok_data.get('bookmarked', {}))
        print(f"Imported {bookmarked_imported} bookmarked videos")
        
        # Import following
        following_imported = import_following(conn, tiktok_data.get('following', {}))
        print(f"Imported {following_imported} followed authors")
        
        # Save metadata
        save_metadata(conn, tiktok_data)
        if args.verbose:
            print("\nMetadata saved")
    
    # Print summary information
    if args.verbose:
        print("\n==== Import Summary ====")
        with db_connection() as conn:
            cursor = conn.cursor()
            
            # Count videos with length info
            cursor.execute("SELECT COUNT(*) FROM videos WHERE lengthSeconds IS NOT NULL")
            videos_with_length = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM videos")
            total_videos = cursor.fetchone()[0]
            
            # Count uploaded videos
            cursor.execute("SELECT COUNT(*) FROM videos WHERE uploaded = 1")
            uploaded_videos = cursor.fetchone()[0]
            
            print(f"Videos with length info: {videos_with_length}/{total_videos} ({videos_with_length/total_videos*100:.1f}%)")
            print(f"Videos already uploaded: {uploaded_videos}/{total_videos} ({uploaded_videos/total_videos*100:.1f}%)")
            
            # Get stats on video lengths
            cursor.execute(
                "SELECT MIN(lengthSeconds), MAX(lengthSeconds), AVG(lengthSeconds) FROM videos WHERE lengthSeconds IS NOT NULL")
            min_len, max_len, avg_len = cursor.fetchone()
            if min_len is not None:
                print(f"Video length stats: Min={min_len:.1f}s, Max={max_len:.1f}s, Avg={avg_len:.1f}s")
    
    print("Import completed successfully.")

if __name__ == "__main__":
    main()
