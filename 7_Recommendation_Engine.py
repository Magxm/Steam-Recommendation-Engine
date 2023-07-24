import pandas as pd
import ast
import math

# The actual recommendation engine

games_file = "Data_Preprocessed/games_with_metadata_and_review_processing.csv"
games_df = pd.read_csv(games_file)

# We sort the game by positive_reviews * positive_ratio_y to get the most popular games
games_df["popularity"] = games_df["positive_reviews"] * games_df["positive_ratio_y"]
games_df = games_df.sort_values(by="popularity", ascending=False)

# We now load our tag correlation coefficient data
tag_correlation_df = pd.read_csv("Data_Preprocessed/tag_correlation_coefficient.csv")
# We have to split the "tag_pair", which is a string representation of the pair, into Tag1 and Tag 2
tag_correlation_df["Tag 1"] = tag_correlation_df["tag_pair"].apply(
    lambda x: ast.literal_eval(x)[0]
)
tag_correlation_df["Tag 2"] = tag_correlation_df["tag_pair"].apply(
    lambda x: ast.literal_eval(x)[1]
)

# We also load of positive correlation tag pairs
tag_pairs_df = pd.read_csv("Data_Preprocessed/positive_tag_pairs.csv")

# We also need the recommendations done by all users
recommendations_df = pd.read_csv("Data_Raw/recommendations.csv")

# Lastly we load the users
users_df = pd.read_csv("Data_Raw/users.csv")

while True:
    input_user_id = input("Enter your user ID: ")

    if not input_user_id.isdigit():
        print("Invalid user ID. Please try again.")
        continue

    input_user_id = int(input_user_id)

    if not input_user_id in users_df["user_id"].values:
        print("User not found. Please try again.")
        continue

    print("Collecting your reviews...")
    recommendations = recommendations_df[recommendations_df["user_id"] == input_user_id]

    print(f"Found {len(recommendations)} reviews by you!")
    print(f"Calculating your top tags...")
    top_tags = {}
    for _, row in recommendations.iterrows():
        tags = ast.literal_eval(row["tags"])
        for tag in tags:
            if not tag in top_tags:
                top_tags[tag] = 1
            else:
                top_tags[tag] += 1

    top_tags = sorted(top_tags.items(), key=lambda x: x[1], reverse=True)
    # We only want the top 10 tags
    top_tags = top_tags[:10]

    print("Finding the best 20 games for you...")
    recommended_games = []
    recommended_games_worst_score = 0

    # We iterate the games_df which is sorted by popularity to find the best games to recommend
    for _, game in games_df.iterrows():
        game_score = 1
        game_tags = ast.literal_eval(game["tags"])

        matched_tags = []
        for game_tag in game_tags:
            for top_tag in top_tags:
                # If it is a top tag, we just add a straight score of 1.7
                if game_tag == top_tag[0]:
                    game_score *= 1.7
                    matched_tags.append({"tag": game_tag, "score": 1.7})

                # We now check the tag correlation coefficient
                for _, tag_pair in tag_pairs_df.iterrows():
                    correlation_coefficient = tag_correlation_df[]

                    if (
                        tag_pair["Tag 1"] == game_tag
                        and tag_pair["Tag 2"] == top_tag[0]
                    ):
                        game_score *= correlation_coefficient
                        matched_tags.append(
                            {"tag": game_tag, "score": correlation_coefficient}
                        )
                        
        if len(matched_tags) > 0:
            # We mow check if any of the none matched tags fit nicely with the highly matched tags
            for game_tag in game_tags:
                for matched_tag_entry in matched_tags:
                    if game_tag == matched_tag_entry["tag"]:
                        continue
                    
                    if matched_tag_entry["score"] > 0.7:
                        tag1 = min(game_tag, matched_tag_entry["tag"])
                        tag2 = max(game_tag, matched_tag_entry["tag"])
                        
                        tag_pair_coefficient = tag_correlation_df["Tag 1" == tag1]["Tag 2" == tag2]["average_rating"]
                        game_score *= tag_pair_coefficient
                        
        if len(recommended_games) < 20:
            recommended_games.append({"game": game, "score": game_score})
            if game_score < recommended_games_worst_score:
                recommended_games_worst_score = game_score
                
            recommended_games.sort(key=lambda x: x["score"], reverse=True)
        else:
            if game_score > recommended_games_worst_score:
                recommended_games.append({"game": game, "score": game_score})
                recommended_games.sort(key=lambda x: x["score"], reverse=True)
                recommended_games.pop()
                recommended_games_worst_score = recommended_games[-1]["score"] 
        
        #Early out if we found 20 great games
        if len(recommended_games) > 20 and recommended_games_worst_score > 3:
            break
                
    print("Here are your recommended games:")
    for recommended_game in recommended_games:
        print(
            f"{recommended_game['game']['title']} ({recommended_game['game']['app_id']}) - Score: {recommended_game['score']}"
        )