import pandas as pd
from enum import Enum


class SortingCriteria(Enum):
    POSITIVE_RATIO = "Positive Ratio"
    NUMBER_OF_POSITIVE_REVIEWS = "Number of Positive Reviews"


def data_discovery_games(games_file, recommendations_file, criteria, export=False):
    games_df = pd.read_csv(games_file)
    recommendations_df = pd.read_csv(recommendations_file)

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

    # We only show the top 20
    if export:
        games_df.to_csv(
            "Data_Preprocessed/games_with_metadata_and_review_processing.csv"
        )
    else:
        # We sort out games with less than 100 reviews
        games_df = games_df[games_df["total_reviews"] >= 250]

        if criteria == SortingCriteria.POSITIVE_RATIO:
            sorting_column = "positive_ratio_y"
        elif criteria == SortingCriteria.NUMBER_OF_POSITIVE_REVIEWS:
            sorting_column = "positive_reviews"
        else:
            print("Invalid criteria. Please choose from the available options.")
            return

        top_rated_games = games_df.sort_values(by=sorting_column, ascending=False)
        top_rated_games = top_rated_games.head(20)

        print(f"Top-rated games based on {criteria.value}:")
        print("-----------------------------")
        print(top_rated_games[["app_id", "title", sorting_column, "tags"]])


games_file = "Data_Preprocessed/games_with_metadata.csv"
recommendations_file = "Data_Raw/recommendations.csv"

while True:
    user_criteria = input(
        "Choose one of the criteria: 'Positive Ratio', or 'Number of Positive Reviews', or type 'export' to export the data or 'exit' to exit: "
    )

    if user_criteria == "exit":
        break

    if user_criteria == "export":
        data_discovery_games(games_file, recommendations_file, None, True)
        break

    try:
        sorting_criteria = SortingCriteria[user_criteria.upper().replace(" ", "_")]
    except KeyError:
        print("Invalid criteria. Please choose from the available options.")
    else:
        data_discovery_games(games_file, recommendations_file, sorting_criteria)

print("Done!")
