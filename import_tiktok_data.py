#!/usr/bin/env python3

import json
import os
import sys
import sqlite3
import argparse
from pathlib import Path
from dotenv import load_dotenv
from db_models import db_connection, init_db, DB_PATH, _get_db_type, _get_placeholder

try:
    import psycopg2
except ImportError:
    psycopg2 = None

def _get_integrity_error():
    """Returns the appropriate IntegrityError exception for the configured database."""
    if _get_db_type() == 'postgres' and psycopg2:
        return psycopg2.errors.IntegrityError
    return sqlite3.IntegrityError

def _get_insert_or_replace_sql(table, columns, primary_key='id'):
    """Generate INSERT OR REPLACE/ON CONFLICT statement."""
    placeholder = _get_placeholder()
    placeholders = ", ".join([placeholder] * len(columns))
    is_postgres = _get_db_type() == 'postgres'
    
    if table == 'user' and is_postgres:
        table = '"user"'

    if is_postgres:
        updates = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col != primary_key])
        return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT ({primary_key}) DO UPDATE SET {updates}"
    return f"INSERT OR REPLACE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

def _get_insert_or_ignore_sql(table, columns, primary_key='videoId'):
    """Generate INSERT OR IGNORE/ON CONFLICT DO NOTHING statement."""
    placeholder = _get_placeholder()
    placeholders = ", ".join([placeholder] * len(columns))
    if _get_db_type() == 'postgres':
        return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT ({primary_key}) DO NOTHING"
    return f"INSERT OR IGNORE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

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
    
    sql = _get_insert_or_replace_sql('authors', ['id', 'uniqueIds', 'nicknames', 'followerCount', 'heartCount', 'videoCount'])

    for author_id, author in authors_data.items():
        if author_id and author:
            uniqueIds = json.dumps(author.get('uniqueIds', []))
            nicknames = json.dumps(author.get('nicknames', []))
            
            if args.verbose:
                unique_id = author.get('uniqueIds', [])[0] if author.get('uniqueIds') else "Unknown"
                nickname = author.get('nicknames', [])[0] if author.get('nicknames') else unique_id
                print(f"  Importing author: @{unique_id} ({nickname})")
            
            cursor.execute(sql, (
                author_id,
                uniqueIds,
                nicknames,
                author.get('followerCount', 0),
                author.get('heartCount', 0),
                author.get('videoCount', 0)
            ))
            authors_imported += 1
            
            if args.verbose and authors_imported % 100 == 0:
                print(f"  Imported {authors_imported} authors so far...")
    
    conn.commit()
    return authors_imported

def import_videos(conn, videos_data, descriptions_data):
    """Import videos into the database."""
    from db_models import get_video_file_path
    
    cursor = conn.cursor()
    videos_imported = 0
    
    if args.verbose:
        print(f"\nProcessing {len(videos_data)} videos...")
    
    tiktok_dir = os.getenv('TIKTOK_DIR', '')
    if not tiktok_dir:
        print("Warning: TIKTOK_DIR not set in .env file, video lengths will not be calculated")

    columns = ['id', 'authorId', 'createTime', 'diggCount', 'playCount', 'audioId', 'size', 'description', 'lengthSeconds', 'filePath']
    sql = _get_insert_or_replace_sql('videos', columns)

    for video_id, video in videos_data.items():
        if video_id and video:
            description = descriptions_data.get(video_id, '')
            
            if args.verbose:
                create_time = video.get('createTime', 0)
                date_str = "Unknown date"
                if create_time:
                    from datetime import datetime
                    date_str = datetime.fromtimestamp(create_time/1000).strftime('%Y-%m-%d')
                desc_preview = description[:40] + "..." if len(description) > 40 else description
                print(f"  Importing video {video_id} ({date_str}): {desc_preview}")
            
            length_seconds = None
            stored_path = None
            if tiktok_dir:
                try:
                    video_path, length_seconds = get_video_file_path(tiktok_dir, video_id, get_length=True, verbose=args.verbose)
                    if video_path and os.path.exists(video_path):
                        stored_path = os.path.relpath(video_path, start=tiktok_dir)
                    if length_seconds and args.verbose:
                        print(f"    Found video file: {video_path}")
                        print(f"    Video length: {int(length_seconds//60)}:{int(length_seconds%60):02d} ({length_seconds:.2f} seconds)")
                    elif args.verbose and not video_path:
                        print(f"    Video file not found for {video_id}")
                except Exception as e:
                    if args.verbose:
                        print(f"    Error getting length for video {video_id}: {e}")
            
            cursor.execute(sql, (
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
            ))
            videos_imported += 1
    
    conn.commit()
    return videos_imported

def update_video_statuses(conn, video_ids, status_column):
    """Update a status column for a list of videos."""
    cursor = conn.cursor()
    updated_count = 0
    
    if not video_ids:
        return 0

    if args.verbose:
        print(f"\nUpdating '{status_column}' status for {len(video_ids)} videos...")
    
    is_postgres = _get_db_type() == 'postgres'
    true_value = 'TRUE' if is_postgres else '1'
    placeholder = _get_placeholder()
    
    batch_size = 100
    for i in range(0, len(video_ids), batch_size):
        batch_ids = video_ids[i:i + batch_size]
        placeholders = ', '.join([placeholder] * len(batch_ids))
        
        sql = f"UPDATE videos SET {status_column} = {true_value} WHERE id IN ({placeholders})"
        
        cursor.execute(sql, tuple(batch_ids))
        updated_count += cursor.rowcount

    conn.commit()
    return updated_count

def import_following(conn, following_data):
    """Import following authors into the database."""
    cursor = conn.cursor()
    following_imported = 0
    not_found = 0
    
    following_authors = following_data.get('officialAuthorList', [])
    
    if args.verbose:
        print(f"\nProcessing {len(following_authors)} followed authors...")

    sql = _get_insert_or_ignore_sql('following', ['authorId'], primary_key='authorId')
    IntegrityError = _get_integrity_error()

    for author_id in following_authors:
        if author_id:
            try:
                cursor.execute(sql, (author_id,))
                if cursor.rowcount > 0:
                    following_imported += 1
                    if args.verbose and following_imported % 100 == 0:
                        print(f"  Imported {following_imported} followed authors so far...")
                if args.verbose:
                    cursor.execute(f"SELECT uniqueIds FROM authors WHERE id = {_get_placeholder()}", (author_id,))
                    author_info = cursor.fetchone()
                    if author_info and author_info[0]:
                        try:
                            unique_ids = json.loads(author_info[0])
                            if unique_ids:
                                print(f"  Imported followed author: @{unique_ids[0]}")
                        except:
                            pass
            except IntegrityError:
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
    sql = _get_insert_or_replace_sql('user', ['id', 'uniqueId', 'nickname'])
    cursor.execute(sql, (
        user_data.get('id', ''),
        user_data.get('uniqueId', ''),
        user_data.get('nickname', '')
    ))
    conn.commit()
    return True

def save_metadata(conn, tiktok_data):
    """Save metadata about the import."""
    cursor = conn.cursor()
    sql = _get_insert_or_replace_sql('metadata', ['key', 'value'], primary_key='key')
    
    schema_version = tiktok_data.get('schemaVersion', 0)
    cursor.execute(sql, ('schema_version', str(schema_version)))
    
    import_time = str(int(os.path.getmtime(args.facts_json)) if os.path.exists(args.facts_json) else 0)
    cursor.execute(sql, ('import_timestamp', import_time))
    
    conn.commit()

def main():
    global args
    parser = argparse.ArgumentParser(description="Import TikTok data from facts.json into a database.")
    parser.add_argument("--facts-json", help="Path to facts.json file (default is from .env file or [tiktok_dir]/data/.appdata/facts.json).")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show verbose output during import")
    args = parser.parse_args()
    
    load_dotenv()
    
    if not args.facts_json:
        tiktok_dir = os.getenv('TIKTOK_DIR')
        args.facts_json = os.getenv('FACTS_JSON')
        if not args.facts_json and tiktok_dir:
            args.facts_json = os.path.join(tiktok_dir, 'data', '.appdata', 'facts.json')
    
    if not args.facts_json or not os.path.exists(args.facts_json):
        print("Error: facts.json file not found. Please specify --facts-json or set TIKTOK_DIR or FACTS_JSON in .env file.")
        sys.exit(1)
    
    print(f"Importing data from {args.facts_json}...")
    
    init_db()
    
    if args.verbose:
        print(f"Starting import from {args.facts_json}")
        if _get_db_type() == 'sqlite':
            print(f"Database: {os.getenv('DB_PATH', DB_PATH)}")
        else:
            print(f"Database: PostgreSQL at {os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}")

    tiktok_data = load_tiktok_data(args.facts_json)
    
    if args.verbose:
        print("\nTikTok archive contains:")
        print(f"  {len(tiktok_data.get('authors', {}))} authors")
        print(f"  {len(tiktok_data.get('videos', {}))} videos")
        print(f"  {len(tiktok_data.get('likes', {}).get('officialList', []))} liked videos")
        print(f"  {len(tiktok_data.get('bookmarked', {}).get('officialList', []))} bookmarked videos")
        print(f"  {len(tiktok_data.get('following', {}).get('officialAuthorList', []))} followed authors")
    
    with db_connection() as conn:
        if _get_db_type() == 'sqlite':
            conn.execute("PRAGMA foreign_keys = ON")
        
        if args.verbose:
            print("\n==== Starting database import ====")
        
        authors_imported = import_authors(conn, tiktok_data.get('authors', {}))
        print(f"Imported {authors_imported} authors")
        
        videos_imported = import_videos(conn, tiktok_data.get('videos', {}), tiktok_data.get('videoDescriptions', {}))
        print(f"Imported {videos_imported} videos")
        
        user_imported = import_user_info(conn, tiktok_data.get('user', {}))
        if args.verbose and user_imported:
            user_data = tiktok_data.get('user', {})
            print(f"User info imported: {user_data.get('uniqueId', '')} ({user_data.get('nickname', '')})")
        
        liked_video_ids = tiktok_data.get('likes', {}).get('officialList', [])
        liked_updated = update_video_statuses(conn, liked_video_ids, 'is_liked')
        print(f"Marked {liked_updated} videos as liked")

        bookmarked_video_ids = tiktok_data.get('bookmarked', {}).get('officialList', [])
        bookmarked_updated = update_video_statuses(conn, bookmarked_video_ids, 'is_bookmarked')
        print(f"Marked {bookmarked_updated} videos as bookmarked")
        
        following_imported = import_following(conn, tiktok_data.get('following', {}))
        print(f"Imported {following_imported} followed authors")
        
        save_metadata(conn, tiktok_data)
        if args.verbose:
            print("\nMetadata saved")
    
    if args.verbose:
        print("\n==== Import Summary ====")
        with db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM videos WHERE lengthSeconds IS NOT NULL")
            videos_with_length = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM videos")
            total_videos = cursor.fetchone()[0]
            
            uploaded_check = "uploaded = 1" if _get_db_type() == 'sqlite' else "uploaded = true"
            cursor.execute(f"SELECT COUNT(*) FROM videos WHERE {uploaded_check}")
            uploaded_videos = cursor.fetchone()[0]
            
            if total_videos > 0:
                print(f"Videos with length info: {videos_with_length}/{total_videos} ({videos_with_length/total_videos*100:.1f}%)")
                print(f"Videos already uploaded: {uploaded_videos}/{total_videos} ({uploaded_videos/total_videos*100:.1f}%)")
            
            cursor.execute("SELECT MIN(lengthSeconds), MAX(lengthSeconds), AVG(lengthSeconds) FROM videos WHERE lengthSeconds IS NOT NULL")
            min_len, max_len, avg_len = cursor.fetchone()
            if min_len is not None:
                print(f"Video length stats: Min={min_len:.1f}s, Max={max_len:.1f}s, Avg={avg_len:.1f}s")
    
    print("Import completed successfully.")

if __name__ == "__main__":
    main()