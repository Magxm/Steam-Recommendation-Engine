import pandas as pd
import ast
import math
import seaborn as sns
import matplotlib.pyplot as plt
import os
import json


correlation_df = pd.read_csv("Data_Preprocessed/tag_correlation_coefficient.csv")
print("Unstack the index to create a pivot table for heatmap")
correlation_matrix = correlation_df.unstack().reset_index()
correlation_matrix.columns = ["Tag 1", "Tag 2", "Correlation"]

# Create the heatmap using Seaborn
print("Creating heatmap...")
plt.figure(figsize=(12, 10))
heatmap = sns.heatmap(
    correlation_matrix.pivot(columns=["Tag 1", "Tag 2", "Correlation"]),
    annot=True,
    cmap="coolwarm",
    linewidths=0.5,
)
plt.title("Tag Correlation Heatmap")
plt.show()
