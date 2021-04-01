const express = require('express');
const cors = require('cors');
const candidates = require('./routes/candidates.js');
const PORT = process.env.PORT || 3002;

const app = express();

app.use(express.json());

app.use('/candidates', candidates);

app.listen(PORT, () => {
  console.log(`Ouvindo a porta ${PORT}`);
});