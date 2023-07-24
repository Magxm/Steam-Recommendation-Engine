import pandas as pd
import matplotlib.pyplot as plt


def map_rating_to_score(rating):
    rating_scores = {
        "Overwhelmingly Positive": 5,
        "Very Positive": 4,
        "Mostly Positive": 3,
        "Mixed": 2,
        "Mostly Negative": 1,
        "Very Negative": 0,
        "Overwhelmingly Negative": 0,
    }
    return rating_scores.get(rating, None)


games_file = "Data_Raw/games.csv"
recommendations_file = "Data_Raw/recommendations.csv"

games_df = pd.read_csv(games_file)
recommendations_df = pd.read_csv(recommendations_file)

games_df["rating_score"] = games_df["rating"].apply(map_rating_to_score)

# We count the amount of games with each rating and then show it in a bar chart
rating_counts = games_df["rating_score"].value_counts()
rating_counts = rating_counts.sort_index()

rating_counts.plot.bar()
plt.xlabel("Rating")
plt.ylabel("Count")
plt.title("Rating Distribution for Games")
plt.show()
