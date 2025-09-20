from atproto import Client, client_utils
import os
import re
import cv2
import litellm
from atproto_client.models.app.bsky.embed.defs import AspectRatio
from atproto import models
import db_models
from dotenv import load_dotenv

load_dotenv('.env', override=True)

def get_llm_processed_tags(description, tags):
    """
    Sends the description and tags to an LLM to get a refined list of non-redundant tags.
    """
    # Get LLM configuration from environment variables
    model = os.getenv('LLM_MODEL')
    api_key = os.getenv('LLM_API_KEY')
    api_base = os.getenv('LLM_API_BASE')

    if not all([model, api_key, api_base]):
        print("LLM environment variables (LLM_MODEL, LLM_API_KEY, LLM_API_BASE) are not fully set. Skipping LLM processing.")
        return tags

    # Construct the prompt
    prompt = f"""
    Given the following video description and list of hashtags, please refine the hashtags.
    The goal is to have a concise, relevant, and non-redundant list of tags that would be suitable for a social media post.
    Remove generic or spammy tags (like 'fyp', 'viral', 'foryou').
    Keep the most descriptive and specific tags.
    Return only a comma-separated list of the refined hashtags, without the '#' symbol.

    Description:
    "{description}"

    Hashtags:
    {', '.join(tags)}

    Refined Hashtags:
    """

    try:
        response = litellm.completion(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            api_key=api_key,
            base_url=api_base,
        )
        
        # Extract the comma-separated tags from the response
        refined_tags_str = response.choices[0].message.content.strip()
        
        # Split the string into a list of tags
        refined_tags = [tag.strip() for tag in refined_tags_str.split(',') if tag.strip()]
        
        print(f"LLM refined tags: {refined_tags}")
        return refined_tags

    except Exception as e:
        print(f"An error occurred during LLM processing: {e}")
        return tags # Fallback to original tags

def main():
    client = Client()
    profile = client.login(os.getenv('BLUESKY_USERNAME'), os.getenv('BLUESKY_PASSWORD'))
    print('Welcome,', profile.display_name)


    video_db = db_models.get_next_unuploaded_video(os.getenv('SOURCE_TYPE'), os.getenv('AUTHOR_ID'))
    if not video_db:
        print("No unuploaded videos found.")
        exit(0)

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

    if not isinstance(file_path, str):
        file_path = str(file_path)

    video_path = os.path.join(tiktok_dir, file_path)

    if not os.path.exists(video_path):
        print(f"Video file not found at path: {video_path}")
        print('Marking Video as uploaded so I dont keepy trying to upload it TODO add aditional field for broken links')
        db_models.mark_video_uploaded(video_db[0])
        exit(1)

    raw_description = video_db[7]  # description is at index 7
    tags = re.findall(r'#(\w+)', raw_description)
    description = re.sub(r'#\w+', '', raw_description).strip()
    description = re.sub(r'\s+', ' ', description).strip()

    # Get LLM processed tags
    tags = get_llm_processed_tags(description, tags)

    author = db_models.get_author(video_db[1])  # authorid is at index 1
    if author:
        def clean(val):
            if isinstance(val, str) and val.startswith('[') and val.endswith(']'):
                val = val.strip('[]').strip('"\'')
            if isinstance(val, list):
                val = ', '.join(str(v) for v in val)
            return val
        author_nickname = clean(author[2])
        author_handle = clean(author[1])
        creator_info = f"Author: {author_nickname} Handle: {author_handle}\n\n"
    else:
        creator_info = ""

    char_limit = 300
    default_hashtags = ['meme', 'tiktok', 'archive']

    should_add_defaults = False
    if not tags:
        should_add_defaults = True
        print("No hashtags found, adding default hashtags")
    else:
        current_hashtags_text = ' '.join([f"#{tag}" for tag in tags])
        defaults_text = ' '.join([f"#{tag}" for tag in default_hashtags])
        potential_total_length = len(creator_info) + len(description) + len(current_hashtags_text) + len(defaults_text) + len(tags) + len(default_hashtags)
        if potential_total_length <= char_limit:
            should_add_defaults = True

    if should_add_defaults:
        tags.extend(default_hashtags)
        seen = set()
        unique_tags = []
        for tag in tags:
            if tag not in seen:
                seen.add(tag)
                unique_tags.append(tag)
        tags = unique_tags

    hashtags_text = ' '.join([f"#{tag}" for tag in tags])
    non_description_length = len(creator_info) + len(hashtags_text) + len(tags)

    while non_description_length > char_limit and tags:
        fyp_tag_to_remove = next((tag for tag in tags if tag.lower().startswith('fyp')), None)
        
        if fyp_tag_to_remove:
            tags.remove(fyp_tag_to_remove)
        else:
            tags.pop()

        hashtags_text = ' '.join([f"#{tag}" for tag in tags])
        non_description_length = len(creator_info) + len(hashtags_text) + len(tags)

    remaining_chars = char_limit - non_description_length

    if len(description) > remaining_chars:
        description = description[:remaining_chars]

    text_builder = client_utils.TextBuilder()
    text_builder.text(creator_info)
    if description:
        text_builder.text(description.strip())
        text_builder.text(' ')

    for tag in tags:
        text_builder.tag(f"#{tag}", tag)
        text_builder.text(' ')

    print(f"Path: {video_path}")
    print(f"Tags: {tags}")
    print(f"Author: {author[2] if author else 'Unknown'}")
    print(f"Description: {description}")
    print(f"Built text: {text_builder.build_text()}")

    cap = cv2.VideoCapture(video_path)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    cap.release()
    aspect_ratio = AspectRatio(width=width, height=height)

    with open(video_path, 'rb') as f:
        video = f.read()

    video_alt = creator_info + description

    upload_resp = client.upload_blob(video)
    video_blob_ref = upload_resp.blob
    print(f"Video blob ref: {video_blob_ref}")

    video_embed = models.AppBskyEmbedVideo.Main(
        video=video_blob_ref,
        alt=video_alt,
        aspect_ratio=aspect_ratio,
    )

    post = client.send_post(
        text_builder,
        embed=video_embed,
    )
    print(post)
    db_models.mark_video_uploaded(video_db[0])


if __name__ == '__main__':
    main()
