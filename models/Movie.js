const connection = require('./connection');

/*
	"_id" : ObjectId("604fd79f364be01f2d3a3403"),
	"title" : "Home Alone",
	"category" : [
		"family",
		"comedy"
	],
	"imdbRating" : 7.4
*/

const getNewMovie = (movieData) => {
  const {id, title, category, imdbRating} = movieData;

  return {
    id,
    title,
    category,
    imdbRating
  }
}

const getAll = async () => {
  return connection()
  .then((db) => db.collection('movies').find().toArray())
  .then((movies) =>
    movies.map(({_id, title, category, imdbRating}) => getNewMovie({
      id: _id,
      title,
      category,
      imdbRating  
    }))
  );
}

module.exports = {
  getAll
}