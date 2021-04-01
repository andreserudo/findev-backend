const express = require('express');
const Candidates = require('../models/Candidates');

const app = express();

app.use((req, res, next) => {
	//Qual site tem permissão de realizar a conexão, no exemplo abaixo está o "*" indicando que qualquer site pode fazer a conexão
    res.header("Access-Control-Allow-Origin", "*");
	//Quais são os métodos que a conexão pode realizar na API   
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");    
    app.use(cors());
    next();
});

app.get('/all', async (_req, res) => {
  const movies = await Candidates.getAll();

  res.status(200).json(movies);
});

app.post('/new', (req, res) => {
  console.log(req.body);
  const arrayOfCandidates = req.body;

  Candidates.insertNewCandidates(arrayOfCandidates);

  res.status(200).json({ message: 'oi'});
})

app.get('/match/:id', async (req, res) => {
  const filters = req.body;
  const { id } = req.params;

  const matches = await Candidates.matchCandidates(filters, id);

  res.status(200).json(matches);

});


app.delete('/remove', async (req, res) => {
  await Candidates.removeAll();

  res.status(200).json({message: "coleção foi resetada."})
})

module.exports = app;
