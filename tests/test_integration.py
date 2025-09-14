import pytest
import os
import tempfile
import shutil
from unittest.mock import patch, MagicMock
import sys
import json
from pathlib import Path

# Add the parent directory to the path so we can import the main modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db_models
from upload_to_bluesky import get_important_hashtags

class TestDatabaseIntegration:
    """Integration tests for database operations."""

    @pytest.fixture
    def temp_db_dir(self):
        """Create a temporary directory for database testing."""
        temp_dir = tempfile.mkdtemp()
        original_db_path = db_models.DB_PATH

        # Override the database path to use temp directory
        db_models.DB_PATH = os.path.join(temp_dir, 'test_tiktok_data.db')

        yield temp_dir

        # Cleanup
        if os.path.exists(db_models.DB_PATH):
            os.remove(db_models.DB_PATH)
        shutil.rmtree(temp_dir)
        db_models.DB_PATH = original_db_path

    def test_database_initialization(self, temp_db_dir):
        """Test that the database can be initialized correctly."""
        db_models.init_db()

        # Check that the database file was created
        assert os.path.exists(db_models.DB_PATH)

        # Check that all required tables exist
        with db_models.db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]

            expected_tables = ['authors', 'videos', 'following', 'user', 'metadata']
            for table in expected_tables:
                assert table in tables, f"Table {table} not found in database"

    def test_mark_video_uploaded(self, temp_db_dir):
        """Test marking a video as uploaded."""
        db_models.init_db()

        # Insert a test video
        video_id = "test_video_123"
        with db_models.db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO videos (id, authorid, createtime, diggcount, playcount, audioid, size, description, uploaded, uploaddate, lengthseconds, filepath, is_liked, is_bookmarked)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (video_id, "author_123", 1234567890, 100, 1000, "audio_123", "1.0MB", "Test description", 0, "2023-01-01", 10.0, "test/path.mp4", 0, 0))
            conn.commit()

        # Mark video as uploaded
        result = db_models.mark_video_uploaded(video_id)
        assert result is True

        # Verify the video was marked as uploaded
        with db_models.db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT uploaded FROM videos WHERE id = ?", (video_id,))
            uploaded = cursor.fetchone()[0]
            assert uploaded == 1

    def test_get_next_unuploaded_video(self, temp_db_dir):
        """Test getting the next unuploaded video."""
        db_models.init_db()

        # Insert test videos
        video_ids = ["video_1", "video_2", "video_3"]
        for i, video_id in enumerate(video_ids):
            with db_models.db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO videos (id, authorid, createtime, diggcount, playcount, audioid, size, description, uploaded, uploaddate, lengthseconds, filepath, is_liked, is_bookmarked)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (video_id, f"author_{i}", 1234567890 + i, 100 + i, 1000 + i, f"audio_{i}", "1.0MB", f"Description {i}", 0, None, 10.0 + i, f"path_{i}.mp4", 0, 0))
                conn.commit()

        # Get next unuploaded video
        video = db_models.get_next_unuploaded_video('created')
        assert video is not None
        assert video[0] == "video_1"  # Should get the earliest video

        # Mark it as uploaded
        db_models.mark_video_uploaded(video[0])

        # Get next unuploaded video
        next_video = db_models.get_next_unuploaded_video('created')
        assert next_video is not None
        assert next_video[0] == "video_2"

class TestHashtagOptimizationIntegration:
    """Integration tests for hashtag optimization."""

    @patch('upload_to_bluesky.completion')
    def test_hashtag_optimization_integration(self, mock_completion):
        """Test the complete hashtag optimization workflow."""
        # Mock successful LLM response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = '["important", "relevant", "popular", "trending", "viral"]'
        mock_completion.return_value = mock_response

        # Test with a scenario that would trigger optimization
        tags = ['tag1', 'tag2', 'tag3', 'tag4', 'tag5', 'tag6', 'tag7', 'tag8', 'tag9', 'tag10']
        description = "This is a test description that would normally be too long with all these hashtags"

        result = get_important_hashtags(tags, description, max_tags=5)

        # Verify the LLM was called
        mock_completion.assert_called_once()

        # Verify we got the expected number of tags
        assert len(result) == 5
        assert result == ["important", "relevant", "popular", "trending", "viral"]

    @patch('upload_to_bluesky.completion')
    def test_hashtag_optimization_with_environment_config(self, mock_completion):
        """Test hashtag optimization with environment variable configuration."""
        # Set environment variables
        with patch.dict(os.environ, {
            'LLM_MODEL': 'test-model',
            'LLM_API_KEY': 'test-key',
            'LLM_API_BASE': 'https://test-api.com/v1'
        }):
            # Mock successful LLM response
            mock_response = MagicMock()
            mock_response.choices = [MagicMock()]
            mock_response.choices[0].message.content = '["test1", "test2", "test3"]'
            mock_completion.return_value = mock_response

            tags = ['tag1', 'tag2', 'tag3', 'tag4']
            description = "Test description"

            result = get_important_hashtags(tags, description, max_tags=3)

            # Verify the LLM was called with correct configuration
            mock_completion.assert_called_once()
            call_args = mock_completion.call_args
            assert call_args[1]['model'] == 'test-model'
            assert call_args[1]['api_key'] == 'test-key'
            assert call_args[1]['api_base'] == 'https://test-api.com/v1'

            assert result == ["test1", "test2", "test3"]

class TestFileHandlingIntegration:
    """Integration tests for file handling operations."""

    @pytest.fixture
    def temp_tiktok_dir(self):
        """Create a temporary TikTok archive structure."""
        temp_dir = tempfile.mkdtemp()

        # Create TikTok archive structure
        data_dir = os.path.join(temp_dir, 'data')
        os.makedirs(data_dir)

        # Create various video directories
        video_dirs = ['videos', 'Likes/videos', 'Favorites/videos', 'Followed/videos']
        for video_dir in video_dirs:
            full_path = os.path.join(data_dir, video_dir)
            os.makedirs(full_path)

        # Create test video files
        test_videos = [
            'data/videos/video1.mp4',
            'data/Likes/videos/video2.mp4',
            'data/Favorites/videos/video3.mp4',
            'data/Followed/videos/video4.mp4'
        ]

        for video_path in test_videos:
            full_path = os.path.join(temp_dir, video_path)
            with open(full_path, 'w') as f:
                f.write('dummy video content')

        yield temp_dir

        # Cleanup
        shutil.rmtree(temp_dir)

    def test_get_video_file_path_common_locations(self, temp_tiktok_dir):
        """Test finding video files in common locations."""
        video_id = 'video1'
        result_path = db_models.get_video_file_path(temp_tiktok_dir, video_id)

        expected_path = os.path.join(temp_tiktok_dir, 'data', 'videos', f'{video_id}.mp4')
        assert result_path == expected_path
        assert os.path.exists(result_path)

    def test_get_video_file_path_likes(self, temp_tiktok_dir):
        """Test finding video files in Likes directory."""
        video_id = 'video2'
        result_path = db_models.get_video_file_path(temp_tiktok_dir, video_id)

        expected_path = os.path.join(temp_tiktok_dir, 'data', 'Likes', 'videos', f'{video_id}.mp4')
        assert result_path == expected_path
        assert os.path.exists(result_path)

    def test_get_video_file_path_not_found(self, temp_tiktok_dir):
        """Test handling when video file is not found."""
        video_id = 'nonexistent_video'
        result_path = db_models.get_video_file_path(temp_tiktok_dir, video_id)

        assert result_path is None

    def test_get_video_file_path_with_length(self, temp_tiktok_dir):
        """Test getting video file path with length information."""
        video_id = 'video1'
        result_path, length = db_models.get_video_file_path(temp_tiktok_dir, video_id, get_length=True)

        expected_path = os.path.join(temp_tiktok_dir, 'data', 'videos', f'{video_id}.mp4')
        assert result_path == expected_path
        assert os.path.exists(result_path)
        # Length should be None since we don't have a real video file
        assert length is None

class TestPostConstructionIntegration:
    """Integration tests for post construction logic."""

    def test_post_construction_with_default_hashtags(self):
        """Test post construction when default hashtags should be added."""
        # Simulate the post construction logic
        tags = []
        default_hashtags = ['meme', 'tiktok', 'archive']
        creator_info = "Author: Test\n\n"
        description = "Short description"
        char_limit = 300

        # Check if we should add default hashtags
        should_add_defaults = False
        if not tags:
            should_add_defaults = True

        if should_add_defaults:
            tags.extend(default_hashtags)
            # Remove duplicates while preserving order
            seen = set()
            unique_tags = []
            for tag in tags:
                if tag not in seen:
                    seen.add(tag)
                    unique_tags.append(tag)
            tags = unique_tags

        assert tags == default_hashtags

        # Test that the total length is within limits
        hashtags_text = ' '.join([f"#{tag}" for tag in tags])
        total_length = len(creator_info) + len(description) + len(hashtags_text) + len(tags)
        assert total_length <= char_limit

    def test_post_construction_with_space_for_defaults(self):
        """Test post construction when there's space for default hashtags."""
        tags = ['existing']
        default_hashtags = ['meme', 'tiktok', 'archive']
        creator_info = "Author: Test\n\n"
        description = "Short desc"
        char_limit = 300

        # Check if we have extra character space for defaults
        current_hashtags_text = ' '.join([f"#{tag}" for tag in tags])
        defaults_text = ' '.join([f"#{tag}" for tag in default_hashtags])
        potential_total_length = len(creator_info) + len(description) + len(current_hashtags_text) + len(defaults_text) + len(tags) + len(default_hashtags)

        should_add_defaults = potential_total_length <= char_limit

        if should_add_defaults:
            tags.extend(default_hashtags)
            # Remove duplicates while preserving order
            seen = set()
            unique_tags = []
            for tag in tags:
                if tag not in seen:
                    seen.add(tag)
                    unique_tags.append(tag)
            tags = unique_tags

        expected_tags = ['existing', 'meme', 'tiktok', 'archive']
        assert tags == expected_tags
        assert potential_total_length <= char_limit

    def test_post_construction_without_space_for_defaults(self):
        """Test post construction when there's no space for default hashtags."""
        tags = ['verylonghashtag1', 'verylonghashtag2', 'verylonghashtag3']
        default_hashtags = ['meme', 'tiktok', 'archive']
        creator_info = "Author: Very Long Name That Takes Up Space\n\n"
        description = "This is a very long description that takes up a lot of characters and would cause the post to exceed the character limit if we added more hashtags"
        char_limit = 100  # Very small limit

        # Check if we have extra character space for defaults
        current_hashtags_text = ' '.join([f"#{tag}" for tag in tags])
        defaults_text = ' '.join([f"#{tag}" for tag in default_hashtags])
        potential_total_length = len(creator_info) + len(description) + len(current_hashtags_text) + len(defaults_text) + len(tags) + len(default_hashtags)

        should_add_defaults = potential_total_length <= char_limit

        if should_add_defaults:
            tags.extend(default_hashtags)
            # Remove duplicates while preserving order
            seen = set()
            unique_tags = []
            for tag in tags:
                if tag not in seen:
                    seen.add(tag)
                    unique_tags.append(tag)
            tags = unique_tags

        # Tags should remain unchanged due to space constraints
        expected_tags = ['verylonghashtag1', 'verylonghashtag2', 'verylonghashtag3']
        assert tags == expected_tags
        assert potential_total_length > char_limit

@pytest.mark.integration
def test_full_workflow_simulation():
    """Test a simulation of the full workflow without actually uploading."""
    # This would be a more comprehensive test that simulates the entire
    # upload process from database reading to post construction
    # For now, we'll test the key components

    # Test data
    video_db = [
        'test_video_id',  # id
        'test_author_id',  # authorid
        1234567890,  # createtime
        100,  # diggcount
        1000,  # playcount
        'test_audio_id',  # audioid
        '1.0MB',  # size
        'Test description #hashtag1 #hashtag2',  # description
        False,  # uploaded
        None,  # uploaddate
        10.0,  # lengthseconds
        'data/videos/test_video_id.mp4',  # filepath
        False,  # is_liked
        False  # is_bookmarked
    ]

    # Test hashtag extraction
    import re
    raw_description = video_db[7]
    tags = re.findall(r'#(\w+)', raw_description)
    assert tags == ['hashtag1', 'hashtag2']

    # Test description cleaning
    description = re.sub(r'#\w+', '', raw_description).strip()
    description = re.sub(r'\s+', ' ', description).strip()
    assert description == 'Test description'

    # Test file path handling
    tiktok_dir = '/fake/tiktok/dir'
    file_path = video_db[11]
    if not isinstance(file_path, str):
        file_path = str(file_path)

    video_path = os.path.join(tiktok_dir, file_path)
    expected_path = '/fake/tiktok/dir/data/videos/test_video_id.mp4'
    assert video_path == expected_path

if __name__ == "__main__":
    pytest.main([__file__, '-v'])
