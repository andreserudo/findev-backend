const express = require('express');
const cors = require('cors');
const candidates = require('./routes/candidates.js');
const PORT = process.env.PORT || 3002;

const app = express();

app.use(express.json());
app.use((req, res, next) => {
	//Qual site tem permissão de realizar a conexão, no exemplo abaixo está o "*" indicando que qualquer site pode fazer a conexão
    res.header("Access-Control-Allow-Origin", "*");
	//Quais são os métodos que a conexão pode realizar na API
    res.header("Access-Control-Allow-Methods", 'GET,PUT,POST,DELETE');    
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");    
    app.use(cors());
    next();
});
app.use('/candidates', candidates);

app.listen(PORT, () => {
  console.log(`Ouvindo a porta ${PORT}`);
});