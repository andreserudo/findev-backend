const { MongoClient } = require('mongodb');

const MONGODB_URL = 'mongodb+srv://dbSerudo:d2E9UNsWwHHrhVpf@cluster0.zdfop.mongodb.net/myFirstDatabase?retryWrites=true&w=majority';

const connection = () => {
  return MongoClient
    .connect(MONGODB_URL, {
      urlNewParser: true,
      useUnifiedTopology: true
    })
    .then((conn) => conn.db('movies'))
    .catch((err) => {
      console.error(err);
      process.exit();
    });
}

module.exports = connection;