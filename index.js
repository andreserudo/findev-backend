const express = require('express');
const Movie = require('./models/Movie');

const app = express();

app.get('/movies', async (_req, res) => {
  const movies = await Movie.getAll();

  res.status(200).json(movies);
});

const PORT = process.env.PORT || 3000;


app.listen(PORT);
// app.listen(PORT, () => {
//   console.log(`Ouvindo a porta ${PORT}`);
// });