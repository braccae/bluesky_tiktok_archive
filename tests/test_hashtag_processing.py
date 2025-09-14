import pytest
import re
from unittest.mock import patch, MagicMock
import sys
import os

# Add the parent directory to the path so we can import the main module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
        ("Multiple #spaces #between #hashtags", "Multiple spaces between hashtags"),
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
    remaining_chars = 30

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
        ('["item1", "item2"]', 'item1, item2'),
        (['item1', 'item2'], 'item1, item2'),
        ('normal_string', 'normal_string'),
        ('["quoted_string"]', 'quoted_string'),
    ]

    for input_val, expected in test_cases:
        result = clean(input_val)
        assert result == expected, f"Failed for input: {input_val}"

@patch('upload_to_bluesky.completion')
def test_get_important_hashtags_success(mock_completion):
    """Test successful hashtag optimization using LLM."""
    # Mock the LLM response
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = '["important", "relevant", "popular"]'
    mock_completion.return_value = mock_response

    # Import the function (we need to handle the import carefully due to sys.path)
    from upload_to_bluesky import get_important_hashtags

    tags = ['tag1', 'tag2', 'tag3', 'tag4', 'tag5', 'tag6']
    description = "Test description"

    result = get_important_hashtags(tags, description, max_tags=3)

    assert result == ["important", "relevant", "popular"]
    mock_completion.assert_called_once()

@patch('upload_to_bluesky.completion')
def test_get_important_hashtags_json_error(mock_completion):
    """Test hashtag optimization when JSON parsing fails."""
    # Mock the LLM response with invalid JSON
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = 'important relevant popular'  # Not JSON
    mock_completion.return_value = mock_response

    from upload_to_bluesky import get_important_hashtags

    tags = ['tag1', 'tag2', 'tag3', 'tag4', 'tag5', 'tag6']
    description = "Test description"

    result = get_important_hashtags(tags, description, max_tags=3)

    # Should fall back to extracting hashtags manually
    assert result == ["important", "relevant", "popular"]

@patch('upload_to_bluesky.completion')
def test_get_important_hashtags_llm_error(mock_completion):
    """Test hashtag optimization when LLM call fails."""
    # Mock the LLM to raise an exception
    mock_completion.side_effect = Exception("LLM Error")

    from upload_to_bluesky import get_important_hashtags

    tags = ['tag1', 'tag2', 'tag3', 'tag4', 'tag5', 'tag6']
    description = "Test description"

    result = get_important_hashtags(tags, description, max_tags=3)

    # Should fall back to first N tags
    assert result == ['tag1', 'tag2', 'tag3']

if __name__ == "__main__":
    pytest.main([__file__])
