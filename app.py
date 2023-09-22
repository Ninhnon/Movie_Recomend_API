from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
import os
import json
import pandas as pd
from utilities import predict_new_user
from utilities import predict_user_has_rating
from utilities import get_all_movies_has_rating
from utilities import get_movies_by_genre_utilities

# Init app
app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))

# Database
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'db.sqlite')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Init db
db = SQLAlchemy(app)

# Init marshmallow
ma = Marshmallow(app)

# User Model
class User(db.Model):
    __tablename__ = 'user'
    userId = db.Column(db.Integer, primary_key = True)
    username = db.Column(db.String(50))
    email = db.Column(db.String(50))
    password = db.Column(db.String(50))
    users = db.relationship('Movie', secondary = 'user_movie')
    user_movies = db.relationship('UserMovie', backref = 'user', lazy = True)
    
    def __init__(self, username, email, password):
        self.username = username
        self.email = email
        self.password = password

# Movie Model
class Movie(db.Model):
    __tablename__ = 'movie'
    movieId = db.Column(db.Integer, primary_key = True)
    movieTitle = db.Column(db.String(50))
    movieGenre = db.Column(db.String(50))
    movieImage = db.Column(db.String(300))
    movies = db.relationship('User', secondary = 'user_movie')
    
    def __init__(self, movieTitle, movieGenre, movieImage):
        self.movieTitle = movieTitle
        self.movieGenre = movieGenre
        self.movieImage = movieImage

# UserMovie Model
class UserMovie(db.Model):
    __tablename__ = 'user_movie'
    userId = db.Column(db.Integer, db.ForeignKey('user.userId'), primary_key = True)
    movieId = db.Column(db.Integer, db.ForeignKey('movie.movieId'), primary_key = True)
    rating = db.Column(db.Float)
    isFavorited = db.Column(db.Boolean, default = False)
    isWatched = db.Column(db.Boolean, default = True)

# User Schema
class UserSchema(ma.Schema):
    class Meta:
        fields = ('userId', 'username', 'email', 'password')
        model = User

# Movie Schema
class MovieSchema(ma.Schema):
    class Meta:
        fields = ('movieId', 'movieTitle', 'movieGenre', 'movieImage')
        model = Movie

# UserMovie Schema
class UserMovieSchema(ma.Schema):
    class Meta:
        fields = ('userId', 'movieId', 'rating', 'isFavorited', 'isWatched')
        model = UserMovie

# Init Schema
user_schema = UserSchema()
users_schema = UserSchema(many = True)

movie_schema = MovieSchema()
movies_schema = MovieSchema(many = True)

usermovie_schema = UserMovieSchema()
usermovies_schema = UserMovieSchema(many = True)

# route
@app.route('/')
def home():
    strHome = "Movie Database"
    return strHome

@app.route('/users', methods = ['GET'])
def get_all_users():
    # Get all users
    users = User.query.all()
    
    if not users:  # If no movies exist in the database
        # Load data from the movies.json file
        with open('./data/users.json') as json_file:
            users_data = json.load(json_file)

        # Save movies to the database
        for user_data in users_data:
            user = User(
                userId = user_data['userId']
            )

            db.session.add(user)
        
        db.session.commit()
        
        # Retrieve movies from the database
        users = Movie.query.all()
    
    # Serialize the user data using the users schema
    result = users_schema.dump(users)
    
    # Return the serialized user as JSON response
    return jsonify(result)

# Get user
@app.route('/users/<int:user_id>', methods = ['GET'])
def get_user(user_id):
    # Get the user with the given ID from the database
    user = User.query.get(user_id)
    
    if user:
        # Serialize the user data using the user schema
        result = user_schema.dump(user)
        
        result['movies'] = usermovies_schema.dump(user.user_movies)
        
        # Return the serialized user as JSON response
        return jsonify(result)
    else:
        # Return a 404 error if the user is not found
        return jsonify({'message': 'User not found'}), 404

# DELETE USER
# Get user
@app.route('/users/<int:user_id>', methods = ['DELETE'])
def delete_user(user_id):
    # Get the user with the given ID from the database
    user = User.query.get(user_id)
    
    if user:
            # Delete the user_movie(s)
            db.session.delete(user)
            db.session.commit()
            return jsonify({'message': 'User movies deleted successfully'}), 200
    else:
            # Return a 404 error if the user_movie is not found
            return jsonify({'message': 'User not found'}), 404
    
@app.route('/users', methods = ['POST'])
def create_user():
    # Get the user data from the request body
    data = request.get_json()
    
    # Extract the required fields from the user data
    username = data['username']
    email = data['email']
    password = data['password']
    
    # Create a new User instance with the extracted data
    new_user = User(username = username, email = email, password = password)
    
    # Add the new user to the session
    db.session.add(new_user)
    
    # Commit the changes to the database
    db.session.commit()
    
    # Serialize the new user data using the user schema
    result = user_schema.dump(new_user)
    
    # Return the serialized user as JSON response with a 201 status code
    return jsonify(result), 201

@app.route('/users/<int:user_id>', methods = ['PUT'])
def update_user(user_id):
    # Get user need to update
    user = User.query.get(user_id)
    
    # Get the user data from the request body
    data = request.get_json()
    
    # Extract the required fields from the user data
    username = data.get('username')
    email = data.get('email')
    password = data.get('password')
    
    # Update user
    if username:
        user.username = username
    if email:
        user.email = email
    if password:
        user.password = password
    
    # Commit the changes to the database
    db.session.commit()
    
    # Serialize the new user data using the user schema
    result = user_schema.dump(user)
    
    # Return the serialized user as JSON response with a 201 status code
    return jsonify(result), 201

# @app.route('/users', methods=['DELETE'])
# def delete_all_users():
#     # Delete all users from the database
#     db.session.query(User).delete()
#     db.session.commit()
    
#     # Return a success message as JSON response
#     return jsonify({'message': 'All users have been deleted'}), 200

# MOVIE
# @app.route('/movies')
# def get_movies():
#     movies = Movie.query.all()

#     if not movies:  # If no movies exist in the database
#         # Load data from the movies.json file
#         with open('./data/movies.json') as json_file:
#             movies_data = json.load(json_file)

#         # Save movies to the database
#         for movie_data in movies_data:
#             movie = Movie(
#                 movieTitle = movie_data['title'],
#                 movieGenre = movie_data['genres'],
#                 movieImage = movie_data['image']
#             )
#             db.session.add(movie)
        
#         db.session.commit()
        
#         # Retrieve movies from the database
#         movies = Movie.query.all()  

#     # Serialize the movies using MovieSchema
#     result = movies_schema.dump(movies)

#     return jsonify(result)

# USER MOVIE

@app.route('/user_movies', methods = ['GET'])
def get_all_user_movies():
    # Get all user_movie
    user_movies = UserMovie.query.all()
    
    if not user_movies:  # If no user_movies exist in the database
        # Load data from the user_movies.json file
        with open('./data/ratings.json') as json_file:
            user_movies_data = json.load(json_file)

        # Save user_movies to the database
        for data in user_movies_data:
            user_movie = UserMovie(
                userId = data['userId'],
                movieId = data['movieId'],
                rating = data['rating']
            )
            db.session.add(user_movie)
        
        db.session.commit()
        
        # Retrieve user_movies from the database
        user_movies = UserMovie.query.all()
    
    # Serialize the user_movie data using the user_movies schema
    result = usermovies_schema.dump(user_movies)
    
    # REturn the serialized user_movie as JSON response
    return jsonify(result)

@app.route('/user_movies/<int:user_movie_id>', methods = ['GET'])
def get_user_movie(user_movie_id):
    # Get the user_movie with the given ID from the database
    user_movies = UserMovie.query.filter_by(userId = user_movie_id).all()
    
    if user_movies:
        # Serialize the user_movie data using the user schema
        result = usermovies_schema.dump(user_movies)
        
        # Return the serialized user_movie as JSON response
        return jsonify(result)
    else:
        # Return a 404 error if the user_movie is not found
        return jsonify({'message': 'User not found'}), 404

# DELETE
@app.route('/user_movies/<int:user_movie_id>', methods = ['DELETE'])
def delete_user_movie(user_movie_id):
    user_movies = UserMovie.query.filter_by(userId = user_movie_id).all()

    if user_movies:
        # Delete the user_movie(s)
        for user_movie in user_movies:
            db.session.delete(user_movie)
        db.session.commit()
        return jsonify({'message': 'User movies deleted successfully'}), 200
    else:
        # Return a 404 error if the user_movie is not found
        return jsonify({'message': 'User not found'}), 404

@app.route('/user_movies', methods = ['POST'])
def create_user_movie():
    # Get the user_movie data from the request body
    data = request.get_json()
    
    # Extract the required fields from the user_movie data
    userId = data['userId']
    movieId = data['movieId']
    rating = data['rating']
    isFavorited = data['isFavorited']
    isWatched = data['isWatched']
    
    # Create a new UserMovie instance with the extracted data
    new_user_movie = UserMovie(userId = userId, movieId = movieId, rating = rating, isFavorited = isFavorited, isWatched = isWatched)
    
    # Add the new user_movie to the session
    db.session.add(new_user_movie)
    
    # Commit the changes to the database
    db.session.commit()
    
    # Serialize the new user_movie data using the user schema
    result = usermovie_schema.dump(new_user_movie)
    
    # Return the serialized user_movie as JSON response with a 201 status code
    return jsonify(result), 201

@app.route('/user_movies/<int:user_movie_id>', methods = ['PUT'])
def update_user_movie(user_movie_id):
    # Get the user_movie data from the request body
    data = request.get_json()
    
    # Extract the required fields from the user_movie data
    movieId = data.get('movieId')
    rating = data.get('rating')
    isFavorited = data.get('isFavorited')
    isWatched = data.get('isWatched')
    
    # Get user_movie need to update
    user_movie = UserMovie.query.filter_by(userId = user_movie_id, movieId = movieId).first()
    
    # Update user_movie
    if movieId:
        user_movie.movieId = movieId
    if rating:
        user_movie.rating = rating
    if isFavorited:
        user_movie.isFavorited = isFavorited
    if isWatched:
        user_movie.isWatched = isWatched
    
    # Commit the changes to the database
    db.session.commit()
    
    # Serialize the new user_movie data using the user_movie schema
    result = usermovie_schema.dump(user_movie)
    
    # Return the serialized user_movie as JSON response with a 201 status code
    return jsonify(result), 201

# Predict new user
@app.route('/predict_new_user', methods=['POST'])
def predict_another():
    data = request.get_json()
    try:
        sample = data['genres']
    except KeyError:
        return jsonify({'error': 'No text sent'})
    prediction = predict_new_user(sample)
    recommendations = []
    for index, row in prediction.iterrows():
        recommendation = {
            "movieId": row['movieId'],
            "movieTitle": row['movieTitle'],
            "movieGenre": row['movieGenre'],
            "mean_rating": row['mean_rating'],
            "movieImage": row['movieImage']
        }
        recommendations.append(recommendation)

    return recommendations

# Predict user has rating
@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json()
    try:
        sample = data['userId']
    except KeyError:
        return jsonify({'error': 'No text sent'})
    prediction = predict_user_has_rating(sample)
    
    recommendations = []
    for index, row in prediction.iterrows():
        recommendation = {
            "movieId": row['movieId'],
            "movieTitle": row['movieTitle'],
            "movieGenre": row['movieGenre'],
            "mean_rating": row['mean_rating'],
            "movieImage": row['movieImage']
        }
        recommendations.append(recommendation)

    return recommendations

# MOVIE

# Get all movies has rating
@app.route('/movies', methods=['GET'])
def get_all_movies():
    data = get_all_movies_has_rating()
    
    movies_data = []
    for index, row in data.iterrows():
        movie_data = {
            "movieId": row['movieId'],
            "movieTitle": row['movieTitle'],
            "movieGenre": row['movieGenre'],
            "mean_rating": row['mean_rating'],
            "movieImage": row['movieImage']
        }
        movies_data.append(movie_data)

    return movies_data

# Get movie by genre
@app.route('/movies/<string:genre>', methods=['GET'])
def get_movies_by_genre(genre):
    data = get_movies_by_genre_utilities(genre)
    
    movies_data = []
    for index, row in data.iterrows():
        movie_data = {
            "movieId": row['movieId'],
            "movieTitle": row['movieTitle'],
            "movieGenre": row['movieGenre'],
            "mean_rating": row['mean_rating'],
            "movieImage": row['movieImage']
        }
        movies_data.append(movie_data)

    return movies_data

# Run server
if __name__ == "__main__":
    app.run(debug = True)