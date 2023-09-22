import sqlite3
import pandas as pd
import os
import tensorflow as tf
import numpy as np
import json

# Read database
connection = sqlite3.connect('./db.sqlite')

query_movies = "select * from movie"
movies_df = pd.read_sql_query(query_movies, connection)

query_ratings = "select * from user_movie"
ratings_df = pd.read_sql_query(query_ratings, connection)

connection.close()

user_ids = ratings_df['userId'].unique().tolist()
user2user_encoded = {x : i for i, x in enumerate(user_ids)}
user_encoded2user = {i : x for i, x in enumerate(user_ids)}

movie_ids = ratings_df['movieId'].unique().tolist()
movie2movie_encoded = {x : i for i, x in enumerate(movie_ids)}
movie_endcoded2movie = {i : x for i, x in enumerate(movie_ids)}

# A new user has just signed in
from sklearn.metrics.pairwise import cosine_similarity

# One-hot encode the movie genres
genres_df = movies_df['movieGenre'].str.get_dummies()

# Combine the ratings and genres data
movies_data = pd.concat([movies_df['movieId'], genres_df], axis = 1)

ratings_data = pd.merge(ratings_df, movies_data, on = 'movieId')

# Update
ratings_count = ratings_data.groupby('movieId')['rating'].count().reset_index()
ratings_count.columns = ['movieId', 'ratings_count']
ratings_data = ratings_data.merge(ratings_count, on = 'movieId', how = 'left')
# Create a user-item matrix
user_item_matrix = ratings_data.pivot_table(index = 'userId', columns = 'movieId', values = 'rating').fillna(0)

# Compute the movie similarities
movie_similarities = cosine_similarity(user_item_matrix.T)

# Load model
path_model = './model'
model_keras = tf.keras.models.load_model(path_model)

def get_all_movies_has_rating(top_n = 20):
  # Caculate mean rating of each movie
  movie_ratings = ratings_data.groupby('movieId')['rating'].mean().to_frame()
  
  # Handle mean ratings with two column: movieId and mean_rating
  result_data = movie_ratings.reset_index().rename(columns={'rating': 'mean_rating'})
  
  # Format the mean_rating column
  result_data['mean_rating'] = result_data['mean_rating'].apply(lambda x: f"{x:.1f}")
  
  # Merge necessary column
  result_data = pd.merge(result_data, movies_df[['movieId', 'movieTitle', 'movieGenre', 'movieImage']], on='movieId')
  
  # Sort by mean rating in descending order and get top 20
  result_data = result_data.sort_values(by='mean_rating', ascending=False)
  result_data = result_data.head(top_n)
  
  # return ratings_mean
  return result_data

def get_movies_by_genre_utilities(genre, top_n = 20):
    # Caculate mean rating of each movie
  movie_ratings = ratings_data.groupby('movieId')['rating'].mean().to_frame()
  
  # Handle mean ratings with two column: movieId and mean_rating
  result_data = movie_ratings.reset_index().rename(columns={'rating': 'mean_rating'})
  
  # Format the mean_rating column
  result_data['mean_rating'] = result_data['mean_rating'].apply(lambda x: f"{x:.1f}")
  
  # Merge necessary column
  result_data = pd.merge(result_data, movies_df[['movieId', 'movieTitle', 'movieGenre', 'movieImage']], on='movieId')
  
  # Filter by genre
  result_data = result_data[result_data['movieGenre'].str.contains(genre, case=False)]
  
  # Sort by mean rating in descending order and get top 20
  result_data = result_data.sort_values(by='mean_rating', ascending=False)
  result_data = result_data.head(top_n)
  
  # return ratings_mean
  return result_data

def predict_new_user(genres, top_n=10, movie_data = movies_data):
  # Split the genres string into a list of genres
  genres_list = genres.split(',')
  
  # Filter the movie data by the specified genres
  genre_movies_df = movie_data[movie_data[genres_list].isin([1]).any(axis=1)]

  # Compute the average rating for each movie
  movie_ratings = ratings_data.groupby('movieId')['rating'].mean().to_frame()

  # Handle mean ratings with two column: movieId and mean_rating
  ratings_mean = movie_ratings.reset_index()
  ratings_mean = movie_ratings.reset_index().rename(columns={'rating': 'mean_rating'})

  # Compute the number of ratings for each movie
  movie_ratings_count = ratings_data.groupby('movieId')['rating'].count().to_frame()
  
  # # Combine the movie ratings and counts data
  movie_data = pd.merge(movie_ratings, movie_ratings_count, on='movieId')
  movie_data = pd.merge(movie_data, movies_df[['movieId', 'movieTitle']], on='movieId')
  movie_data = pd.merge(movie_data, genre_movies_df, on='movieId')
  
  # Merge with the rating data
  movie_data = pd.merge(movie_data, ratings_data[['movieId', 'rating', 'ratings_count']], on='movieId')
  
  # Compute the weighted average rating for each movie
  movie_data['weighted_rating'] = (movie_data['rating'] * movie_data['ratings_count']) / (movie_data['ratings_count'] + 1000)

  # Compute the similarity between the selected genres and all movies
  genre_vector = genre_movies_df.mean().values.reshape(1, -1)
  similarity_scores = cosine_similarity(genre_vector, movie_data.iloc[:, 4:-2].values)[0]

  # Sort the movies by their similarity score and weighted rating
  movie_data['similarity_score'] = similarity_scores
  movie_data = movie_data.sort_values(['similarity_score', 'weighted_rating'], ascending=False)
  movie_data.drop_duplicates(subset='movieId', keep='first', inplace=True)
  
  # Result
  recommended_movies = movie_data
  recommended_movies = pd.merge(recommended_movies, ratings_mean[['movieId', 'mean_rating']], on='movieId')
  recommended_movies = recommended_movies[recommended_movies['mean_rating'] > 4.0]
  recommended_movies = pd.merge(recommended_movies, movies_df[['movieId', 'movieGenre', 'movieImage']], on='movieId')
  recommended_movies = recommended_movies.head(top_n)

  # Format the mean_rating column
  recommended_movies['mean_rating'] = recommended_movies['mean_rating'].apply(lambda x: f"{x:.1f}")

  # return json.dumps(recommended_movies[['movieId', 'movieTitle', 'movieGenre', 'mean_rating', 'movieImage']].to_dict('records'),indent=4)
  return recommended_movies[['movieId', 'movieTitle', 'movieGenre', 'mean_rating', 'movieImage']]

# User has ratings before
def predict_user_has_rating(user_id, top_n = 10):
  # Compute the average rating for each movie
  movie_ratings = ratings_data.groupby('movieId')['rating'].mean().to_frame()
  
  # Handle mean ratings with two column: movieId and mean_rating
  ratings_mean = movie_ratings.reset_index()
  ratings_mean = movie_ratings.reset_index().rename(columns = {'rating' : 'mean_rating'})
  
  # Recommend movie
  movies_watched_by_user = ratings_df[ratings_df.userId == user_id]
  movies_not_watched = movies_df[
    ~movies_df['movieId'].isin(movies_watched_by_user.movieId.values)]['movieId']
  movies_not_watched = list(
    set(movies_not_watched).intersection(set(movie2movie_encoded.keys())))
  
  movies_not_watched = [[movie2movie_encoded.get(x)] for x in movies_not_watched]
  user_encoder = user2user_encoded.get(user_id)
  user_movie_array = np.hstack(
      ([[user_encoder]] * len(movies_not_watched), movies_not_watched)
  )

  ratings = model_keras.predict(user_movie_array).flatten()

  top_ratings_indices = ratings.argsort()[-top_n:][::-1]
  recommended_movie_ids = [
      movie_endcoded2movie.get(movies_not_watched[x][0]) for x in top_ratings_indices
  ]
  recommended_movies = movies_df[movies_df['movieId'].isin(recommended_movie_ids)]
  recommended_movies = pd.merge(recommended_movies, ratings_mean[['movieId', 'mean_rating']], on='movieId')
  
  # Format the mean_rating column
  recommended_movies['mean_rating'] = recommended_movies['mean_rating'].apply(lambda x: f"{x:.1f}")

  # return recommended_movies[['movieId', 'title', 'genres', 'mean_rating', 'weighted_rating', 'similarity_score']]
  return recommended_movies[['movieId', 'movieTitle', 'movieGenre', 'mean_rating', 'movieImage']]

if __name__ == "__main__":
  userId = 611
  genres = 'Action'
  movie_recommendation_json = predict_user_has_rating(userId)
  print(movie_recommendation_json)