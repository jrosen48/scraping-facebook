import os
import pandas as pd
import facebook_scraper as fs

# Read the CSV into a DataFrame
d = pd.read_csv("2023-05-24-14-48-56-EDT-Historical-Report-chatGPT-for-Teachers-2022-05-24--2023-05-24.csv")

d = d.loc[251:750]  # Testing

# Split the last number from the URL column and create 'Post ID' column
d['Post ID'] = d['URL'].str.split("/permalink/").str.get(-1)

# number of comments to download -- set this to True to download all comments
MAX_COMMENTS = True

# Create a list to store comment data
comment_data = []

# Iterate through each 'Post ID' in the DataFrame
for index, post_id in enumerate(d['Post ID']):
    try:
        # Get the post for the current 'Post ID'
        gen = fs.get_posts(
            post_urls=[post_id],
            options={"comments": MAX_COMMENTS, "progress": True}
        )
        post = next(gen)

        # Extract the comments part
        comments = post['comments_full']

        # Process comments and replies
        for comment in comments:
            comment_data.append({
                'Post ID': post_id,
                'Comment ID': comment['comment_id'],
                'Comment Text': comment['comment_text'],
                'Author': comment['commenter_url'],
                'Parent Comment ID': None
            })

            # Process replies
            for reply in comment['replies']:
                comment_data.append({
                    'Post ID': post_id,
                    'Comment ID': reply['comment_id'],
                    'Comment Text': reply['comment_text'],
                    'Author': reply['commenter_url'],
                    'Parent Comment ID': comment['comment_id']
                })

    except Exception as e:
        print(f"Error processing post: {post_id}\n{e}")

    # Print message for every 50 rows processed
    if (index + 1) % 50 == 0:
        print(f"Processed {index + 1} rows.")

    # Save the DataFrame every 50 rows
    if (index + 1) % 50 == 0:
        output_folder = 'data-2'
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        output_file = os.path.join(output_folder, f"comments-df-{index + 1}.csv")
        comments_df = pd.DataFrame(comment_data)
        comments_df.to_csv(output_file, index=False)

# Convert comment_data to DataFrame
comments_df = pd.DataFrame(comment_data)

# Display the DataFrame
print(comments_df.head())

# Save the final DataFrame
comments_df.to_csv("comments-df-final.csv", index=False)
