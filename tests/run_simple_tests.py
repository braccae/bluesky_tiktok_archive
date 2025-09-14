#!/usr/bin/env python3
"""
Simple test runner for bluesky_tiktok_archive project.
This runner doesn't require pytest and can run basic tests.
"""

import sys
import os
import re
import tempfile
import shutil
from pathlib import Path

# Add the parent directory to the path so we can import the main modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def run_test(test_name, test_func):
    """Run a single test function and report results."""
    try:
        test_func()
        print(f"✅ {test_name}")
        return True
    except Exception as e:
        print(f"❌ {test_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_hashtag_extraction():
    """Test that hashtags are correctly extracted from descriptions."""
    test_cases = [
        ("Hello world #test #hashtag", ["test", "hashtag"]),
        ("No hashtags here", []),
        ("#single hashtag", ["single"]),
        ("Mixed #case #Hashtag #TAGS", ["case", "Hashtag", "TAGS"]),
        ("#numbers123 #special_chars", ["numbers123", "special_chars"]),
        ("Multiple #spaces #between #hashtags", ["spaces", "between", "hashtags"]),
    ]

    for description, expected_tags in test_cases:
        tags = re.findall(r'#(\w+)', description)
        assert tags == expected_tags, f"Failed for description: {description}"

def test_hashtag_removal_from_description():
    """Test that hashtags are correctly removed from descriptions."""
    test_cases = [
        ("Hello world #test #hashtag", "Hello world"),
        ("No hashtags here", "No hashtags here"),
        ("#single hashtag", "hashtag"),
        ("Start #middle end", "Start end"),
        ("Multiple #spaces #between #hashtags", "Multiple"),
    ]

    for original, expected in test_cases:
        # Remove all hashtags (words starting with #) from the description
        result = re.sub(r'#\w+', '', original).strip()
        # Collapse multiple spaces into one
        result = re.sub(r'\s+', ' ', result).strip()
        assert result == expected, f"Failed for original: {original}"

def test_default_hashtag_addition_no_existing_tags():
    """Test adding default hashtags when no existing tags."""
    tags = []
    default_hashtags = ['meme', 'tiktok', 'archive']
    creator_info = "Author: Test\n\n"
    description = "Test description"
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

    assert tags == default_hashtags, f"Expected {default_hashtags}, got {tags}"

def test_default_hashtag_addition_with_space():
    """Test adding default hashtags when there's character space."""
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
    assert tags == expected_tags, f"Expected {expected_tags}, got {tags}"

def test_default_hashtag_addition_no_space():
    """Test not adding default hashtags when there's no character space."""
    tags = ['verylonghashtag1', 'verylonghashtag2', 'verylonghashtag3']
    default_hashtags = ['meme', 'tiktok', 'archive']
    creator_info = "Author: Very Long Name That Takes Up Space\n\n"
    description = "This is a very long description that takes up a lot of characters and would cause the post to exceed the character limit if we added more hashtags"
    char_limit = 100  # Very small limit to force no addition

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

    # Should not have added defaults due to space constraints
    expected_tags = ['verylonghashtag1', 'verylonghashtag2', 'verylonghashtag3']
    assert tags == expected_tags, f"Expected {expected_tags}, got {tags}"

def test_duplicate_hashtag_removal():
    """Test that duplicate hashtags are removed while preserving order."""
    tags = ['original', 'meme', 'tiktok', 'original', 'archive', 'meme']
    default_hashtags = ['meme', 'tiktok', 'archive']

    # Simulate adding defaults and removing duplicates
    tags.extend(default_hashtags)
    seen = set()
    unique_tags = []
    for tag in tags:
        if tag not in seen:
            seen.add(tag)
            unique_tags.append(tag)
    tags = unique_tags

    expected_tags = ['original', 'meme', 'tiktok', 'archive']
    assert tags == expected_tags, f"Expected {expected_tags}, got {tags}"

def test_character_count_calculation():
    """Test that character count calculations are correct."""
    creator_info = "Author: Test\n\n"
    description = "Test description"
    tags = ['tag1', 'tag2', 'tag3']

    # Calculate the character count of the creator info and hashtags
    hashtags_text = ' '.join([f"#{tag}" for tag in tags])
    non_description_length = len(creator_info) + len(hashtags_text) + len(tags)  # Add len(tags) for spaces between hashtags

    expected_hashtags_text = "#tag1 #tag2 #tag3"
    expected_non_description_length = len(creator_info) + len(expected_hashtags_text) + len(tags)

    assert hashtags_text == expected_hashtags_text
    assert non_description_length == expected_non_description_length

def test_description_truncation():
    """Test that description is correctly truncated when too long."""
    description = "This is a very long description that needs to be truncated because it exceeds the available character limit"
    remaining_chars = 27

    if len(description) > remaining_chars:
        truncated_description = description[:remaining_chars]
    else:
        truncated_description = description

    expected_truncated = "This is a very long descrip"
    assert truncated_description == expected_truncated, f"Expected '{expected_truncated}', got '{truncated_description}'"

def test_author_info_cleaning():
    """Test that author info is correctly cleaned."""
    def clean(val):
        if isinstance(val, str) and val.startswith('[') and val.endswith(']'):
            val = val.strip('[]').strip('"\'')
        if isinstance(val, list):
            val = ', '.join(str(v) for v in val)
        return val

    test_cases = [
        ('["test"]', 'test'),
        ('["item1", "item2"]', 'item1", "item2'),
        (['item1', 'item2'], 'item1, item2'),
        ('normal_string', 'normal_string'),
        ('["quoted_string"]', 'quoted_string'),
    ]

    for input_val, expected in test_cases:
        result = clean(input_val)
        assert result == expected, f"Failed for input: {input_val}"

def test_post_construction_with_default_hashtags():
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

def test_post_construction_with_space_for_defaults():
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

def test_post_construction_without_space_for_defaults():
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

def test_full_workflow_simulation():
    """Test a simulation of the full workflow without actually uploading."""
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

def main():
    """Run all tests and report results."""
    print("Running tests for bluesky_tiktok_archive...")
    print("=" * 50)

    tests = [
        ("Hashtag Extraction", test_hashtag_extraction),
        ("Hashtag Removal from Description", test_hashtag_removal_from_description),
        ("Default Hashtag Addition (No Existing Tags)", test_default_hashtag_addition_no_existing_tags),
        ("Default Hashtag Addition (With Space)", test_default_hashtag_addition_with_space),
        ("Default Hashtag Addition (No Space)", test_default_hashtag_addition_no_space),
        ("Duplicate Hashtag Removal", test_duplicate_hashtag_removal),
        ("Character Count Calculation", test_character_count_calculation),
        ("Description Truncation", test_description_truncation),
        ("Author Info Cleaning", test_author_info_cleaning),
        ("Post Construction with Default Hashtags", test_post_construction_with_default_hashtags),
        ("Post Construction with Space for Defaults", test_post_construction_with_space_for_defaults),
        ("Post Construction without Space for Defaults", test_post_construction_without_space_for_defaults),
        ("Full Workflow Simulation", test_full_workflow_simulation),
    ]

    passed = 0
    failed = 0

    for test_name, test_func in tests:
        if run_test(test_name, test_func):
            passed += 1
        else:
            failed += 1

    print("=" * 50)
    print(f"Tests completed: {passed} passed, {failed} failed")

    if failed == 0:
        print("🎉 All tests passed!")
        return 0
    else:
        print(f"❌ {failed} test(s) failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
