const connection = require('./connection');

/*
	"_id" : ObjectId("604fd79f364be01f2d3a3403"),
  "city": "André"
  "experience": "1+ year"
  "technologies": [
    {
      "name": "Java",
      "is_main_tech": true
    }
  ]
*/


const insertNewCandidates = (candidates) => {
  candidates.forEach(element => {
    const candidate = {
      id_candidate: element.id,
      city: element.city,
      experience: element.experience,
      technologies: element.technologies,
    }    

    connection().then((db) => db.collection('candidates').insertOne(candidate));
  });
}

const getCandidates = (candidateData) => {
  const {id, city, experience, technologies} = candidateData;
  
  return {
    id,
    city,
    experience,
    technologies  
  }
}

const getAll = async () => {
  return connection()
  .then((db) => db.collection('candidates').find().toArray())
  .then((candidates) =>
    candidates.map(({id_candidate, city, experience, technologies}) => getCandidates({
      id: id_candidate,
      city,
      experience,
      technologies  
    }))
  );
}

const removeAll = async () => {
  return connection()
  .then((db) => db.collection('candidates').deleteMany());
}

const getPipeline = (filters, toSkip, limit) => {
  const { 
    city, 
    stack, 
    initialYear, 
    finalYear
  } = filters;

  if (( city === '') && (stack === '') && (initialYear === '') && (finalYear === '')) {
    const pipeline = [
      { '$skip': toSkip },
      { '$limit': limit }
    ];

    return pipeline;
  }

  if (( city !== '') && (stack !== '') && (initialYear === '') && (finalYear === '')) {
    const pipeline = [
      {
        '$match': {
          'city': {
            '$regex': new RegExp(city, 'i')
          },
          'technologies.name': {
            '$regex': new RegExp(stack, 'i')  
          }
        }
      },{
        '$project': {
          '_id': 0
        }
      },
      { '$skip': toSkip },
      { '$limit': limit }      
    ];
  
    return pipeline;  
  }

  if ((( city !== '') || (stack !== '')) && (initialYear === '') && (finalYear === '')) {
    console.log('deveria cair aqui');
    console.log(`${city} - ${stack}`);
    if( stack === '') {
      const pipeline = [
        {
          "$match": {
            "city": {
              '$regex': new RegExp(city, 'i')
            }
          }
        },
        { '$skip': toSkip },
        { '$limit': limit }
      ];
  
      return pipeline;  
    }

    if( city === '') {
      const pipeline = [
        {
          "$match": {
            "technologies.name": {
              '$regex': new RegExp(stack, 'i')
            }
          }
        },
        { '$skip': toSkip },
        { '$limit': limit }
      ];
  
      return pipeline;  
    }


  }


  if (( city !== '') && (stack !== '') && (initialYear !== '') && (finalYear !== '')) {    
    let minimum = Number(initialYear);
    let maximum = Number(finalYear);
    
    if (minimum > maximum) {
      const auxValue = maximum;
      maximum = minimum;
      minimum = auxValue;            
    }
    console.log(`${city} - ${stack}`);    
    console.log(`${minimum} - ${maximum}`);    
    const pipeline = [
      {
        "$match": {
          "city": {
            '$regex': new RegExp(city, 'i')
          },          
          "technologies.name": {
            "$regex": new RegExp(stack, 'i')
          }
        }
      },
      {
        "$addFields": {
          "aux": { "$split": ["$experience", "-"]  }
        }
      },    
      {
        "$addFields": {
          "inicio": { "$toInt": {"$arrayElemAt": ["$aux", 0]} } ,
          "auxfim": { "$arrayElemAt": ["$aux", 1] } 
        }
      },
      {
        "$addFields": {
          "fimsplit": { "$split": ["$auxfim", " "]  }
        }
      },          
      {
        "$addFields": {
          "fim": { "$toInt": {"$arrayElemAt": ["$fimsplit", 0]}}
        }
      },
      {
        "$match": {
          "inicio": { "$gte": minimum},
          "fim": { "$lte": maximum},
        }
      },
      {
        "$project": {
          "_id": 0
        }
      },
      { '$skip': toSkip },
      { '$limit': limit }      
    ];
  
    return pipeline;  
  }



}

const matchCandidates = async (filters, page) => {
  const matches = [];
  const limit = 5;
  const toSkip = (limit * page) - limit;
  const pipeline = getPipeline(filters, toSkip, limit);
  // db.candidates.aggregate([{$match: {city: {$regex: /^rio/i}}}]).pretty();
  
  console.log(pipeline);
  const result = await connection()
    .then((db) => db.collection('candidates').aggregate(pipeline))
    .then((candidates) =>
    candidates.map(({id_candidate, city, experience, technologies}) => getCandidates({
      id: id_candidate,
      city,
      experience,
      technologies  
    }))
  );    

  await result.forEach(item => {
    matches.push(item);    
  });
  //console.log(matches[0]);  

  return matches;
}

module.exports = {
  getAll,
  insertNewCandidates,
  removeAll,
  matchCandidates
}