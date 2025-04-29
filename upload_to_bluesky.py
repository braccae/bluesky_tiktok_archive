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

    video_path, video_length = db_models.get_video_file_path(os.getenv('TIKTOK_DIR'), video_db['id'], True)
    if not video_path:
        print("Video file not found.")
        exit(0)
    ## parse description of video from database, spearate hashtags from text save tags as a list and text as a string
    import re
    raw_description = video_db['description']
    # Extract tags: all words starting with #
    tags = re.findall(r'#(\w+)', raw_description)
    # Remove all hashtags (words starting with #) from the description
    description = re.sub(r'#\w+', '', raw_description).strip()
    # Optionally, collapse multiple spaces into one
    description = re.sub(r'\s+', ' ', description).strip()

    # Get author information from the database
    author = db_models.get_author(video_db['authorId'])
    if author:
        # Remove brackets/quotes if these are lists or strings with brackets
        def clean(val):
            if isinstance(val, str) and val.startswith('[') and val.endswith(']'):
                val = val.strip('[]').strip('"\'')
            if isinstance(val, list):
                val = ', '.join(str(v) for v in val)
            return val
        author_nickname = clean(author['nicknames'])
        author_handle = clean(author['uniqueIds'])
        creator_info = f"Author: {author_nickname} Handle: {author_handle}\n\n"
    else:
        creator_info = ""


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
    print(f"Author: {author['nicknames'] if author else 'Unknown'}")
    print(f"Description: {description}")
    print(f"Built text: {text_builder.build_text()}")
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
    db_models.mark_video_uploaded(video_db['id'])


if __name__ == '__main__':
    main()