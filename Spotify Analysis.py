# Databricks notebook source
# DBTITLE 1,My first Query
# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   rabi_catalog.discover_x.spotify_2023

# COMMAND ----------

spotify_df = _sqldf.toPandas()

display(spotify_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT artists_name, COUNT(*) AS track_count
# MAGIC FROM rabi_catalog.discover_x.spotify_2023
# MAGIC GROUP BY artists_name
# MAGIC ORDER BY track_count DESC
# MAGIC LIMIT 10

# COMMAND ----------



# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd
import matplotlib.pyplot as plt

# Read the data from the table
df = spark.table("rabi_catalog.discover_x.spotify_2023")

# Group the data by released month and count the number of occurrences for each month
grouped_df = df.groupBy("released_month").agg(F.count("*").alias("count"))

# Convert the Spark DataFrame to a Pandas DataFrame
pandas_df = grouped_df.toPandas()

# Plot the data in a line chart with a purple line
plt.plot(pandas_df["released_month"], pandas_df["count"], color="purple")

# Set the x-axis and y-axis labels and the chart title
plt.xlabel("Month")
plt.ylabel("Number of Songs Released")
plt.title("Total Songs Released per Month")

# Show the chart
plt.show()


# COMMAND ----------

top10_df = spotify_df.sort_values(by="in_spotify_playlists", ascending=False).head(10)
display(top10_df[['track_name', 'artists_name', 'in_spotify_playlists']])

# COMMAND ----------

def displayTopSongsInPlaylist(platform_column) :
  top10_df = spotify_df.sort_values(by=platform_column, ascending=False).head(10)
  display(top10_df[['track_name', 'artists_name', platform_column]])

# COMMAND ----------

displayTopSongsInPlaylist("in_apple_playlists")

# COMMAND ----------

displayTopSongsInPlaylist("in_deezer_playlists")

# COMMAND ----------


import matplotlib.pyplot as plt

# Get the top 10 songs in each playlist by sorting based on the playlist column
spotify_top10 = spotify_df.sort_values(by='in_spotify_playlists', ascending=False).head(10)
apple_top10 = spotify_df.sort_values(by='in_apple_playlists', ascending=False).head(10)
deezer_top10 = spotify_df.sort_values(by='in_deezer_playlists', ascending=False).head(10)


# Get the average valence of the top 10 songs in each playlist
spotify_avg_valence = spotify_top10['valence_%'].mean()
apple_avg_valence = apple_top10['valence_%'].mean()
deezer_avg_valence = deezer_top10['valence_%'].mean()


# Plot the data
plt.bar(['Spotify', 'Apple Music', 'Deezer'], [spotify_avg_valence, apple_avg_valence, deezer_avg_valence], alpha=0.5)
plt.ylabel('Average Valence %')
plt.title('Average Valence of Top 10 Songs in Playlists')
plt.show()

# COMMAND ----------

import matplotlib.pyplot as plt

fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(10, 5))

# Get the top 10 songs in each playlist by sorting based on the playlist column
spotify_top10 = spotify_df.sort_values(by='in_spotify_playlists', ascending=False).head(10)
apple_top10 = spotify_df.sort_values(by='in_apple_playlists', ascending=False).head(10)
deezer_top10 = spotify_df.sort_values(by='in_deezer_playlists', ascending=False).head(10)

# Get the average valence and danceability of the top 10 songs in each playlist
spotify_avg_valence = spotify_top10['valence_%'].mean()
spotify_avg_danceability = spotify_top10['danceability_%'].mean()

apple_avg_valence = apple_top10['valence_%'].mean()
apple_avg_danceability = apple_top10['danceability_%'].mean()

deezer_avg_valence = deezer_top10['valence_%'].mean()
deezer_avg_danceability = deezer_top10['danceability_%'].mean()

# Plot the average valence of the top 10 songs in each playlist
axs[0].bar(['Spotify', 'Apple Music', 'Deezer'], [spotify_avg_valence, apple_avg_valence, deezer_avg_valence])
axs[0].set_title('Average Valence of Top 10 Songs in Playlists')
axs[0].set_ylabel('Average Valence %')

# Plot the danceability of the top 10 songs in each playlist
axs[1].bar(['Spotify', 'Apple Music', 'Deezer'], [spotify_avg_danceability, apple_avg_danceability, deezer_avg_danceability])
axs[1].set_title('Average Danceability of Top 10 Songs in Playlists')
axs[1].set_ylabel('Average Danceability %')

fig.suptitle('Top 10 Songs in Playlists', fontsize=14)

plt.show()
