import pandas as pd
import json

games_metadata_file = "Data_Raw/games_metadata.json"
games_file = "Data_Raw/games.csv"
output_csv_file = "Data_Preprocessed/games_with_metadata.csv"

metadata_df = pd.read_json(games_metadata_file, lines=True)
metadata_df = metadata_df.drop(columns=["description"])
games_df = pd.read_csv(games_file)

merged_df = pd.merge(
    metadata_df, games_df, left_on="app_id", right_on="app_id", how="left"
)

merged_df.to_csv(output_csv_file, index=False)
