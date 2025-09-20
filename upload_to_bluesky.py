from atproto import Client, client_utils
import os

from atproto_client.models.app.bsky.embed.defs import AspectRatio
import db_models
from dotenv import load_dotenv

load_dotenv('.env', override=True)

def main():
    client = Client()
    profile = client.login(os.getenv('BLUESKY_USERNAME'), os.getenv('BLUESKY_PASSWORD'))
    print('Welcome,', profile.display_name)


    video_db = db_models.get_next_unuploaded_video(os.getenv('SOURCE_TYPE'), os.getenv('AUTHOR_ID'))
    if not video_db:
        print("No unuploaded videos found.")
        exit(0)

    # Debug: Print the database row structure (commented out for production)
    # print(f"Database row type: {type(video_db)}")
    # print(f"Database row length: {len(video_db)}")
    # print(f"Database row contents: {video_db}")

    # Get file path from database and ensure it's a string
    file_path = video_db[11]  # filepath is at index 11 based on database schema
    if file_path is None:
        print(f"Video {video_db[0]} has no file path in the database.")
        print('Marking Video as uploaded so I dont keepy trying to upload it TODO add aditional field for broken links')
        db_models.mark_video_uploaded(video_db[0])
        exit(1)

    tiktok_dir = os.getenv('TIKTOK_DIR')
    if not tiktok_dir:
        print("TIKTOK_DIR environment variable not set")
        exit(1)
    # Get file path from database and ensure it's a string
    file_path = video_db[11]  # filepath is at index 11 based on database schema
    if file_path is None:
        print(f"Video {video_db[0]} has no file path in the database.")
        print('Marking Video as uploaded so I dont keepy trying to upload it TODO add aditional field for broken links')
        db_models.mark_video_uploaded(video_db[0])
        exit(1)

    # Convert to string if it's not already
    if not isinstance(file_path, str):
        file_path = str(file_path)

    video_path = os.path.join(tiktok_dir, file_path)
    # print(f"Constructed video path: {video_path}")

    if not os.path.exists(video_path):
        print(f"Video file not found at path: {video_path}")
        print('Marking Video as uploaded so I dont keepy trying to upload it TODO add aditional field for broken links')
        db_models.mark_video_uploaded(video_db[0])
        exit(1)
    ## parse description of video from database, spearate hashtags from text save tags as a list and text as a string
    import re
    raw_description = video_db[7]  # description is at index 7
    # Extract tags: all words starting with #
    tags = re.findall(r'#(\w+)', raw_description)
    # Remove all hashtags (words starting with #) from the description
    description = re.sub(r'#\w+', '', raw_description).strip()
    # Optionally, collapse multiple spaces into one
    description = re.sub(r'\s+', ' ', description).strip()

    # Get author information from the database
    author = db_models.get_author(video_db[1])  # authorid is at index 1
    if author:
        # Remove brackets/quotes if these are lists or strings with brackets
        def clean(val):
            if isinstance(val, str) and val.startswith('[') and val.endswith(']'):
                val = val.strip('[]').strip('"\'')
            if isinstance(val, list):
                val = ', '.join(str(v) for v in val)
            return val
        author_nickname = clean(author[2])  # nicknames is at index 2
        author_handle = clean(author[1])  # uniqueids is at index 1
        creator_info = f"Author: {author_nickname} Handle: {author_handle}\n\n"
    else:
        creator_info = ""

    # Define the character limit
    char_limit = 300

    # Add default hashtags if none exist or if there's extra character space
    default_hashtags = ['meme', 'tiktok', 'archive']



    # Check if we should add default hashtags
    should_add_defaults = False
    if not tags:
        # Add defaults if no hashtags exist
        should_add_defaults = True
        print("No hashtags found, adding default hashtags")
    else:
        # Check if we have extra character space for defaults
        current_hashtags_text = ' '.join([f"#{tag}" for tag in tags])
        defaults_text = ' '.join([f"#{tag}" for tag in default_hashtags])
        potential_total_length = len(creator_info) + len(description) + len(current_hashtags_text) + len(defaults_text) + len(tags) + len(default_hashtags)
        current_total_length = len(creator_info) + len(description) + len(current_hashtags_text) + len(tags)



        if potential_total_length <= char_limit:
            should_add_defaults = True


    if should_add_defaults:
        original_tags = tags.copy()
        tags.extend(default_hashtags)
        # Remove duplicates while preserving order
        seen = set()
        unique_tags = []
        for tag in tags:
            if tag not in seen:
                seen.add(tag)
                unique_tags.append(tag)
        tags = unique_tags



    # Calculate the character count of the creator info and hashtags
    hashtags_text = ' '.join([f"#{tag}" for tag in tags])
    non_description_length = len(creator_info) + len(hashtags_text) + len(tags)  # Add len(tags) for spaces between hashtags

    # Truncate tags if they (plus creator_info) are already too long
    while non_description_length > char_limit and tags:
        # Prioritize removing tags starting with 'fyp'
        fyp_tag_to_remove = next((tag for tag in tags if tag.lower().startswith('fyp')), None)
        
        if fyp_tag_to_remove:
            tags.remove(fyp_tag_to_remove)
        else:
            tags.pop()  # if no 'fyp' tags are left, remove from the end

        hashtags_text = ' '.join([f"#{tag}" for tag in tags])
        non_description_length = len(creator_info) + len(hashtags_text) + len(tags)

    # Calculate the remaining characters for the description
    remaining_chars = char_limit - non_description_length

    # Truncate the description if it's too long
    if len(description) > remaining_chars:
        description = description[:remaining_chars]

    text_builder = client_utils.TextBuilder()
    # Build the post text with description and hashtags using facets
    # Add creator info at the beginning, then description
    text_builder.text(creator_info)
    if description:
        text_builder.text(description.strip())
        text_builder.text(' ')

    for tag in tags:
        # Add hashtag with facet
        text_builder.tag(f"#{tag}", tag)
        text_builder.text(' ')

    # For debugging
    print(f"Path: {video_path}")
    print(f"Tags: {tags}")
    print(f"Author: {author[2] if author else 'Unknown'}")
    print(f"Description: {description}")
    print(f"Built text: {text_builder.build_text()}")

    import cv2
    # Dynamically get aspect ratio from video
    cap = cv2.VideoCapture(video_path)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    cap.release()
    aspect_ratio = AspectRatio(width=width, height=height)

    with open(video_path, 'rb') as f:
        video = f.read()  # Binary video to attach.
    # Alt text includes author info and description without hashtags
    video_alt = creator_info + description

    # Upload the video as a blob first
    upload_resp = client.upload_blob(video)
    video_blob_ref = upload_resp.blob
    print(f"Video blob ref: {video_blob_ref}")

    from atproto import models
    video_embed = models.AppBskyEmbedVideo.Main(
        video=video_blob_ref,
        alt=video_alt,
        aspect_ratio=aspect_ratio,
    )

    # Send the post with the video embed
    post = client.send_post(
        text_builder,
        embed=video_embed,
    )
    print(post)
    #client.like(post.uri, post.cid)
    db_models.mark_video_uploaded(video_db[0])


if __name__ == '__main__':
    main()
