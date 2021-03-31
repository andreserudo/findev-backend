const express = require('express');
const candidates = require('./routes/candidates.js');
const PORT = process.env.PORT || 3000;

const app = express();

app.use(express.json());
app.use('/candidates', candidates);

app.listen(PORT, () => {
  console.log(`Ouvindo a porta ${PORT}`);
});