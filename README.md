# Bluesky TikTok Archive Uploader

A Python tool to upload videos from your TikTok data archive to Bluesky. This application imports your TikTok archive data into SQLite for easier access and uploads videos to Bluesky one at a time.

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/bluesky_tiktok_archive.git
   cd bluesky_tiktok_archive
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Copy the example environment file and edit it with your details:
   ```bash
   cp .env.example .env
   # Edit .env with your favorite text editor
   ```

## Application Structure

The application is divided into three main components:

1. **Importer** (`import_tiktok_data.py`): Imports data from your TikTok archive's `facts.json` into a SQLite database
2. **Database Models** (`db_models.py`): Defines the database schema and provides helper functions for database operations
3. **Uploader** (`upload_to_bluesky.py`): Reads the database and configuration to upload videos to Bluesky

## Setup and Usage

### 1. Configure Settings

Edit the `.env` file with your TikTok archive location and Bluesky credentials:

```
# TikTok Archive Configuration
TIKTOK_DIR=/path/to/tiktok/archive

# Upload Configuration
# Source type can be 'created', 'liked', or 'bookmarked'
SOURCE_TYPE=created

# Bluesky Credentials
BLUESKY_USERNAME=your_email@example.com
BLUESKY_PASSWORD=your_password
```

### 2. Import TikTok Data

First, import your TikTok data into the SQLite database:

```bash
python import_tiktok_data.py
```

This will:
- Read the `facts.json` file from your TikTok archive
- Import all metadata into a SQLite database
- Create tables for videos, authors, likes, bookmarks, etc.

### 3. Upload Videos to Bluesky

After importing data, you can start uploading videos:

```bash
python upload_to_bluesky.py
```

By default, this will:
- Upload one video (based on your SOURCE_TYPE setting)
- Mark it as uploaded in the database so it won't be uploaded again
- Wait for the specified delay before attempting the next upload (if you run it again)

#### Upload Command Options

```
python upload_to_bluesky.py [options]

options:
  --source {created,liked,bookmarked}  Source of videos to upload (overrides .env)
  --author-id AUTHOR_ID               Specific author ID to filter by (for created videos)
  --max MAX                           Maximum number of videos to upload (overrides .env)
```

#### Examples

Upload up to 5 videos from your liked videos:
```bash
python upload_to_bluesky.py --source liked --max 5
```

Continuously upload all bookmarked videos (with delay between each):
```bash
python upload_to_bluesky.py --source bookmarked --max 0
```

## How It Works

1. The importer parses the `facts.json` file and imports all relevant data into a SQLite database
2. The database stores videos, authors, likes, bookmarks, and which videos have already been uploaded
3. The uploader:
   - Gets the next unuploaded video of the specified type
   - Locates the actual video file within the TikTok archive
   - Formats a Bluesky post with the video's description, author, and creation date
   - Uploads the video to Bluesky
   - Marks the video as uploaded in the database

## Notes

- This tool is read-only for your TikTok archive - it won't modify any files there
- Videos are uploaded as images/videos to Bluesky
- The uploader respects Bluesky's character limits for posts
- If a video file can't be found, the script will attempt to search recursively through the archive
- The database keeps track of which videos have been uploaded to avoid duplicates

## License

MIT License
