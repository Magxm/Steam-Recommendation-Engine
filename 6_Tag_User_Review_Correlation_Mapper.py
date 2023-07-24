import pandas as pd
import ast
import math
import seaborn as sns
import matplotlib.pyplot as plt
import os
import json


# This works because all integers in the dataset are < 2^31 and we are fine with loosing some float precision
def reduce_memory(df):
    for col in df.columns:
        if df[col].dtype == "int64":
            df[col] = df[col].astype("int32")
        elif df[col].dtype == "float64":
            df[col] = df[col].astype("float32")
    return df


games_file = "Data_Preprocessed/games_with_metadata.csv"
recommendations_file = "Data_Raw/recommendations.csv"
users_file = "Data_Raw/users.csv"

print("Loading files...")
games_df = reduce_memory(pd.read_csv(games_file))
recommendations_df = reduce_memory(pd.read_csv(recommendations_file))
users_df = reduce_memory(pd.read_csv(users_file))

print("Preprocessing tags...")
games_df["tags"] = games_df["tags"].apply(ast.literal_eval)

print("Filtering out games with less than 250 reviews or have no tags...")
games_start_size = games_df.size
games_df = games_df[games_df["user_reviews"] >= 250]
games_df = games_df[games_df["tags"].map(len) > 0]
print(f"Games filtered out: {games_start_size - games_df.size}")
print(games_df.size)
print(games_df.head())

print("Merging games and recommendations...")
merged_df = recommendations_df.merge(
    games_df[["app_id", "tags"]], on="app_id", how="left"
)
print(merged_df.size)
print(merged_df.head())

# If the "Data_Temp/user_likes.csv" file already exists, we load it instead of creating it again
if os.path.exists("Data_Temp/user_likes.csv"):
    print("Loading user likes dataframe...")
    user_likes = pd.read_csv("Data_Temp/user_likes.csv", index_col=0)
    user_likes = reduce_memory(user_likes)
    # user_likes["tags"] = user_likes["tags"].apply(ast.literal_eval)
else:
    print("Creating user likes dataframe...")
    user_likes = (
        merged_df[merged_df["is_recommended"] == True].groupby("user_id")["tags"].sum()
    )
    user_likes.to_csv("Data_Temp/user_likes.csv")

print("Filtering out 0 or empty tags...")
startSize = len(user_likes)
user_likes = user_likes[user_likes["tags"].map(len) > 0]
user_likes = user_likes[user_likes["tags"] != "0"]
print(f"Filtered out {startSize - len(user_likes)} user likes")

print("Reducing memory...")
user_likes = reduce_memory(user_likes)

print("Calculating tag correlation coefficients pre 1...")
tag_correlation = {}
done = 0
total = len(user_likes)

for user, tags in user_likes.iterrows():
    if len(tags) == 1 and type(tags[0]) == str:
        tags = ast.literal_eval(tags[0])

    # print(len(tags))
    # print(type(tags[0]))
    # print(tags[0])
    # print("------------------")
    tmpList = []
    for tag in tags:
        tmpList.append(tag)

    # print(len(tmpList), tmpList)

    for i in range(len(tmpList)):
        for j in range(i + 1, len(tmpList)):
            tag1 = tmpList[i]
            tag2 = tmpList[j]
            print(tag1, tag2)
            pair = tuple(sorted([tag1, tag2]))
            if not pair in tag_correlation:
                tag_correlation[pair] = 1
            else:
                tag_correlation[pair] += 1

    done += 1
    if done % math.floor(total / 100) == 0 or done == 1 or done == total:
        print(f"Progress: {done}/{total} ({len(tag_correlation)} pairs found)")

print(f"Found {len(tag_correlation)} user-tag pairs")
print("Calculating tag correlation coefficients pre 2...")
user_tags_count = {}
done = 0
total = len(user_likes)
for user, tags in user_likes.iterrows():
    for tag in tags:
        pair = (user, tag)
        if not pair in user_tags_count:
            user_tags_count[pair] = 1
        else:
            user_tags_count[pair] += 1

    done += 1
    if done % math.floor(total / 100) == 0 or done == 1 or done == total:
        print(f"Progress: {done}/{total}")

print(f"Found {len(user_tags_count)} tag pairs")
print("Calculating tag correlation coefficients...")
tag_correlation_coefficient = {}
done = 0
total = len(tag_correlation)
for pair, count in tag_correlation.items():
    tag1, tag2 = pair
    if not (user, tag1) in user_tags_count:
        user_tags_count[(user, tag1)] = 0

    if not (user, tag2) in user_tags_count:
        user_tags_count[(user, tag2)] = 0

    tag1_count = sum(user_tags_count[(user, tag1)] for user in user_likes.keys())
    tag2_count = sum(user_tags_count[(user, tag2)] for user in user_likes.keys())
    tag_correlation_coefficient[pair] = count / math.sqrt(tag1_count * tag2_count)

    done += 1
    if done % math.floor(total / 100) == 0 or done == 1 or done == total:
        print(f"Progress: {done}/{total}")


json.dump(
    tag_correlation_coefficient, open("Data_Temp/tag_correlation_coefficient.json", "w")
)

print("Creating tag correlation dataframe...")
correlation_df = pd.DataFrame.from_dict(
    tag_correlation_coefficient, orient="index", columns=["Correlation"]
)

correlation_df.to_csv("Data_Preprocessed/tag_correlation_coefficient.csv")
print("Done!")
