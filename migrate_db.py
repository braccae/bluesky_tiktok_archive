#!/usr/bin/env python3

import sys
from db_models import db_connection, _get_db_type

def column_exists(cursor, table, column, is_postgres):
    """Check if a column exists in a table."""
    if is_postgres:
        # For postgres, query the information_schema
        cursor.execute("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = %s AND column_name = %s
        """, (table, column))
        return cursor.fetchone() is not None
    else:
        # For sqlite, use PRAGMA table_info
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [row[1] for row in cursor.fetchall()]
        return column in columns

def add_columns_to_videos(cursor, is_postgres):
    """Add is_liked and is_bookmarked columns to the videos table."""
    print("Adding new columns to 'videos' table...")
    
    bool_type = "BOOLEAN" if is_postgres else "INTEGER"
    
    if not column_exists(cursor, "videos", "is_liked", is_postgres):
        cursor.execute(f"ALTER TABLE videos ADD COLUMN is_liked {bool_type} DEFAULT {'FALSE' if is_postgres else '0'}")
        print("  Added 'is_liked' column.")
    else:
        print("  'is_liked' column already exists.")
        
    if not column_exists(cursor, "videos", "is_bookmarked", is_postgres):
        cursor.execute(f"ALTER TABLE videos ADD COLUMN is_bookmarked {bool_type} DEFAULT {'FALSE' if is_postgres else '0'}")
        print("  Added 'is_bookmarked' column.")
    else:
        print("  'is_bookmarked' column already exists.")

def migrate_data(conn, table_name, column_name):
    """Migrate data from an old table to a new column in the videos table."""
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"SELECT videoId FROM {table_name}")
    except Exception:
        print(f"Table '{table_name}' not found, skipping migration.")
        return

    print(f"Migrating data from '{table_name}' to '{column_name}' column...")
    
    video_ids = [row[0] for row in cursor.fetchall()]
    
    if not video_ids:
        print(f"  No data to migrate from '{table_name}'.")
        return

    is_postgres = _get_db_type() == 'postgres'
    true_value = 'TRUE' if is_postgres else '1'
    placeholder = '%s' if is_postgres else '?'
    
    updated_count = 0
    batch_size = 100
    for i in range(0, len(video_ids), batch_size):
        batch_ids = video_ids[i:i + batch_size]
        placeholders = ', '.join([placeholder] * len(batch_ids))
        
        sql = f"UPDATE videos SET {column_name} = {true_value} WHERE id IN ({placeholders})"
        
        cursor.execute(sql, tuple(batch_ids))
        updated_count += cursor.rowcount

    print(f"  Migrated {updated_count} records.")

def drop_tables(cursor):
    """Drop the old liked_videos and bookmarked_videos tables."""
    print("Dropping old tables...")
    try:
        cursor.execute("DROP TABLE liked_videos")
        print("  Dropped 'liked_videos' table.")
    except Exception:
        print("  'liked_videos' table not found, skipping.")
        
    try:
        cursor.execute("DROP TABLE bookmarked_videos")
        print("  Dropped 'bookmarked_videos' table.")
    except Exception:
        print("  'bookmarked_videos' table not found, skipping.")

def main():
    """Main function to run the database migration."""
    print("Starting database migration...")
    
    with db_connection() as conn:
        cursor = conn.cursor()
        is_postgres = _get_db_type() == 'postgres'
        
        # Use a transaction
        if not is_postgres:
            cursor.execute("BEGIN")

        try:
            add_columns_to_videos(cursor, is_postgres)
            migrate_data(conn, 'liked_videos', 'is_liked')
            migrate_data(conn, 'bookmarked_videos', 'is_bookmarked')
            drop_tables(cursor)
            
            conn.commit()
            print("\nMigration completed successfully!")
            
        except Exception as e:
            print(f"\nAn error occurred: {e}")
            print("Rolling back changes.")
            conn.rollback()
            sys.exit(1)

if __name__ == "__main__":
    main()
