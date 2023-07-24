import pandas as pd
import ast
import math

games_file = "Data_Preprocessed/games_with_metadata.csv"
recommendations_file = "Data_Raw/recommendations.csv"

print("Loading files...")
games_df = pd.read_csv(games_file)
recommendations_df = pd.read_csv(recommendations_file)

print("Preprocessing...")
game_reviews = recommendations_df.groupby("app_id").agg(
    {"is_recommended": ["sum", "count"]}
)
game_reviews.columns = ["positive_reviews", "total_reviews"]
game_reviews.reset_index(inplace=True)

game_reviews["positive_ratio"] = (
    game_reviews["positive_reviews"] / game_reviews["total_reviews"]
)

games_df = pd.merge(
    games_df, game_reviews, left_on="app_id", right_on="app_id", how="left"
)

# We filter out games with less than 250 reviews to prevent bias
print("Filtering...")
games_df = games_df[games_df["total_reviews"] >= 250]

print("Merging games and recommendations...")
merged_df = pd.merge(games_df, recommendations_df, on="app_id", how="inner")

tag_pairs = {}
print("Grouping by app_id...")
merged_df_grouped = merged_df.groupby("app_id")

done = 0
total = len(merged_df_grouped)
print("Gathering tag pair ratings...")
for _, group in merged_df_grouped:
    average_rating = group["positive_ratio_y"].values[0]
    tags_list_raw = group["tags"].values[0]
    tags_list = ast.literal_eval(tags_list_raw)

    if len(tags_list) >= 2:
        for i in range(len(tags_list)):
            for j in range(i + 1, len(tags_list)):
                tag_pair = tuple(sorted([tags_list[i], tags_list[j]]))
                if not tag_pair in tag_pairs:
                    tag_pairs[tag_pair] = []

                tag_pairs[tag_pair].append(float(average_rating))

    done += 1
    if done % math.floor(total / 5) == 0 or done == 1 or done == total:
        print(f"Progress: {done}/{total}")

print("Calculating average ratings...")
tag_ratings = []
for tag_pair in tag_pairs:
    ratings = tag_pairs[tag_pair]
    if len(ratings) > 10:
        average_rating = sum(ratings) / len(ratings)
        tag_ratings.append([tag_pair, average_rating])

print("Sorting tag pairs...")
sorted_tag_pairs = sorted(tag_ratings, key=lambda x: x[1], reverse=True)
highest_rated_tag_pair = sorted_tag_pairs[0][0]
highest_rated_average_rating = sorted_tag_pairs[0][1]

print("--------------")
# Display results
print("Tags with Highest Average Rating If They Appear Together:")
print(f"Tag Pair: {highest_rated_tag_pair[0]} and {highest_rated_tag_pair[1]}")
print(f"Highest Rated Average Rating: {highest_rated_average_rating:.2f}\n")

# We print the top 10 tag pairs
print("Top 10 Tag Pairs with Highest Average Rating:")
for i in range(10):
    print(
        f"Tag Pair: {sorted_tag_pairs[i][0][0]} and {sorted_tag_pairs[i][0][1]} - Average Rating: {sorted_tag_pairs[i][1]:.2f}"
    )


# Saving result to csv
tag_pairs_df = pd.DataFrame(sorted_tag_pairs, columns=["tag_pair", "average_rating"])
tag_pairs_df.to_csv("Data_Preprocessed/tag_pairs.csv", index=False)
