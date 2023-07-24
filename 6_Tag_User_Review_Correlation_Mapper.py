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

print("Merging games and recommendations...")
merged_df = recommendations_df.merge(
    games_df[["app_id", "tags"]], on="app_id", how="left"
)

# If the "Data_Temp/user_likes.csv" file does not already exists, we create it
if not os.path.exists("Data_Temp/user_likes.csv"):
    print("Creating user likes dataframe...")
    user_likes = (
        merged_df[merged_df["is_recommended"] == True].groupby("user_id")["tags"].sum()
    )
    user_likes.to_csv("Data_Temp/user_likes.csv")

# Loading here to standardize format (bid of a weird pandas thing where loading from csv makes it different types than when we created it)
print("Loading user likes dataframe...")
user_likes = pd.read_csv("Data_Temp/user_likes.csv", index_col=0)
user_likes = reduce_memory(user_likes)

print("Filtering out 0 or empty tags...")
startSize = len(user_likes)
user_likes = user_likes[user_likes["tags"] != "0"]
user_likes = user_likes[user_likes["tags"].map(len) > 0]
print(f"Filtered out {startSize - len(user_likes)} user likes")

print("Reducing memory...")
user_likes = reduce_memory(user_likes)

print("Calculating tag correlation coefficients...")
tag_correlation = {}
user_tags_count = {}
if os.path.exists("Data_Temp/tag_correlation.json") and os.path.exists(
    "Data_Temp/user_tags_count.json"
):
    print("Loading tag correlation coefficients pre data from tmp json...")
    tag_correlation_tmp = json.load(open("Data_Temp/tag_correlation.json", "r"))
    user_tags_count_tmp = json.load(open("Data_Temp/user_tags_count.json", "r"))

    # Changing key string to tuple to be able to use it as a key in the dictionary
    print("Fixing tag correlation coefficients pre data keys...")
    for key, value in tag_correlation_tmp.items():
        tag1 = key[1:-1].split(",")[0].strip()
        tag2 = key[1:-1].split(",")[1].strip()
        tag_correlation = {(tag1, tag2): value}

    for key, value in user_tags_count_tmp.items():
        user_id = int(key[1:-1].split(",")[0].strip())
        tag = key[1:-1].split(",")[1].strip()
        user_tags_count_tmp[(user_id, tag)] = value


else:
    print("Calculating tag correlation coefficients pre 1...")

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
                # print(tag1, tag2)
                pair = tuple(sorted([tag1, tag2]))
                if not pair in tag_correlation:
                    tag_correlation[pair] = 1
                else:
                    tag_correlation[pair] += 1

        done += 1
        if done % math.floor(total / 100) == 0 or done == 1 or done == total:
            print(f"Progress: {done}/{total} ({len(tag_correlation)} pairs found)")

        if done > total / 100:
            break

    print(f"Found {len(tag_correlation)} user-tag pairs")
    print("Calculating tag correlation coefficients pre 2...")

    done = 0
    total = len(user_likes)
    for user, tags in user_likes.iterrows():
        if len(tags) == 1 and type(tags[0]) == str:
            tags = ast.literal_eval(tags[0])

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

    # Changing key tuple to string to be able to save it to json file
    tag_correlation_tmp = {}
    for key, value in tag_correlation.items():
        tag_correlation_tmp[str(key)] = value

    user_tags_count_tmp = {}
    for key, value in user_tags_count.items():
        user_tags_count_tmp[str(key)] = value

    # Saving both tag_correlation and user_tags_count to json files
    json.dump(tag_correlation_tmp, open("Data_Temp/tag_correlation.json", "w"))
    json.dump(user_tags_count_tmp, open("Data_Temp/user_tags_count.json", "w"))

print("Calculating final tag correlation coefficients matrix...")
tag_correlation_coefficient = {}
done = 0
total = len(tag_correlation)
for pair, count in tag_correlation.items():
    tag1, tag2 = pair
    tag1_count = 0
    tag2_count = 0
    for user in user_likes.keys():
        if not (user, tag1) in user_tags_count:
            user_tags_count[(user, tag1)] = 0
        if not (user, tag2) in user_tags_count:
            user_tags_count[(user, tag2)] = 0

        tag1_count += user_tags_count[(user, tag1)]
        tag2_count += user_tags_count[(user, tag2)]

    tag2_count = sum(user_tags_count.get((user, tag2), 0) for user in user_likes.keys())
    if tag1_count > 0 and tag2_count > 0:
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
