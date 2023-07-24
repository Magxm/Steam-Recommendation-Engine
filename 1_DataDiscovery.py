import pandas as pd


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
average_rating = games_df["rating_score"].mean()

total_games = len(recommendations_df)
recommended_games = len(recommendations_df[recommendations_df["is_recommended"] == 1])
percent_recommended = (recommended_games / total_games) * 100

os_columns = ["win", "mac", "linux"]
games_df["os_count"] = games_df[os_columns].sum(axis=1)
average_os_support = games_df["os_count"].mean()

average_price = games_df["price_final"].mean()

steam_deck_games = len(games_df[games_df["steam_deck"] == 1])
total_games = len(games_df)
percent_steam_deck_games = (steam_deck_games / total_games) * 100

total_games_count = total_games
percent_windows_support = (games_df["win"].sum() / total_games) * 100
percent_mac_support = (games_df["mac"].sum() / total_games) * 100
percent_linux_support = (games_df["linux"].sum() / total_games) * 100

print("Data Discovery for Games:")
print("----------------------------")
print(f"Average Rating: {average_rating:.2f}")
print(f"Percentage Recommended: {percent_recommended:.2f}%")
print(f"Average Supported OS: {average_os_support:.2f}")
print(f"Average Price: {average_price:.2f}")
print(f"Percentage of Games Playable on Steam Deck: {percent_steam_deck_games:.2f}%")
print(f"Total Games Count: {total_games_count}")
print(f"Percentage of Games Supporting Windows: {percent_windows_support:.2f}%")
print(f"Percentage of Games Supporting Mac: {percent_mac_support:.2f}%")
print(f"Percentage of Games Supporting Linux: {percent_linux_support:.2f}%")
