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

const getIntervalPipeline = (filters, toSkip, limit) => {
  const { 
    city, 
    stack, 
    initialYear, 
    finalYear
  } = filters;
  // Todos candidatos em que tenham localidade e stack entre os valores
  if (( city !== '') && (stack !== '') && (initialYear !== '') && (finalYear !== '')) {    
    let minimum = Number(initialYear);
    let maximum = Number(finalYear);
    
    if (minimum > maximum) {
      const auxValue = maximum;
      maximum = minimum;
      minimum = auxValue;            
    }
    // console.log(`${city} - ${stack}`);    
    // console.log(`${minimum} - ${maximum}`);    
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
          "aux": { "$split": ["$experience", " "]  }
        }   
      },
      {      
        "$addFields": {
          "primeiroFiltro": { "$arrayElemAt": ["$aux", 0] } 
        }   
      },
      {
        "$addFields": {
          "acharMais": { "$split": ["$primeiroFiltro", "+"]  },
          "acharHifen": { "$split": ["$primeiroFiltro", "-"]  },
        }   
      },
      {      
        "$addFields": {
          "inicioAuxMais": { "$arrayElemAt": ["$acharMais", 0] }, 
          "fimAuxMais": { 
            "$cond": {
              "if": { "$eq": [{"$arrayElemAt": ["$acharMais",0]}, "$primeiroFiltro"]},
              "then": "$primeiroFiltro",
              "else": { "$arrayElemAt": ["$acharMais", 1] }
            }        
          },
          "inicioAuxHifen": { "$arrayElemAt": ["$acharHifen", 0] }, 
          "fimAuxHifen": { 
            "$cond": {
              "if": { "$eq": [{"$arrayElemAt": ["$acharHifen",0]}, "$primeiroFiltro"]},
              "then": "$primeiroFiltro",
              "else": { "$arrayElemAt": ["$acharHifen", 1] }
            }        
          },
        }   
      },
      {
        "$addFields": {
          "inicioMais": { 
            "$cond": { 
              "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
              "then": { "$toInt": "$inicioAuxMais"}, 
              "else": 0
            }
          },
          "fimMais": {
            "$cond": { 
              "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
              "then": 99,
              "else": 0
            }
          },
          "inicioHifen": { 
            "$cond": { 
              "if": { "$lte": [ {"$strLenCP": "$inicioAuxHifen"}, 2 ] },
              "then": { "$toInt": "$inicioAuxHifen"}, 
              "else": 0
            }
          },
          "fimHifen": {
            "$cond": { 
              "if": { "$lte": [ {"$strLenCP": "$fimAuxHifen"}, 2 ] },
              "then": { "$toInt": "$fimAuxHifen"}, 
              "else": 0
            }
          }      
        }
      },{
        "$match": {
          "$or": [              
            { "$and": [
              { "inicioMais": {"$lte": minimum}},
              { "fimMais": {"$gte": maximum}},
            ]},            
            { "$and": [
              { "inicioHifen": {"$gte": minimum}},
              { "fimHifen": {"$lte": maximum}},
            ]}
          ]
        }
      },      
      { '$skip': toSkip },
      { '$limit': limit }           
    ];
  
    return pipeline;  
  }

  // Todos resultados entre os valores
  if (( city === '') && (stack === '') && (initialYear !== '') && (finalYear !== '')) {    
    let minimum = Number(initialYear);
    let maximum = Number(finalYear);
    
    if (minimum > maximum) {
      const auxValue = maximum;
      maximum = minimum;
      minimum = auxValue;            
    }
    // console.log(`${city} - ${stack}`);    
    // console.log(`${minimum} - ${maximum}`);    
    const pipeline = [
      {      
        "$addFields": {
          "aux": { "$split": ["$experience", " "]  }
        }   
      },
      {      
        "$addFields": {
          "primeiroFiltro": { "$arrayElemAt": ["$aux", 0] } 
        }   
      },
      {
        "$addFields": {
          "acharMais": { "$split": ["$primeiroFiltro", "+"]  },
          "acharHifen": { "$split": ["$primeiroFiltro", "-"]  },
        }   
      },
      {      
        "$addFields": {
          "inicioAuxMais": { "$arrayElemAt": ["$acharMais", 0] }, 
          "fimAuxMais": { 
            "$cond": {
              "if": { "$eq": [{"$arrayElemAt": ["$acharMais",0]}, "$primeiroFiltro"]},
              "then": "$primeiroFiltro",
              "else": { "$arrayElemAt": ["$acharMais", 1] }
            }        
          },
          "inicioAuxHifen": { "$arrayElemAt": ["$acharHifen", 0] }, 
          "fimAuxHifen": { 
            "$cond": {
              "if": { "$eq": [{"$arrayElemAt": ["$acharHifen",0]}, "$primeiroFiltro"]},
              "then": "$primeiroFiltro",
              "else": { "$arrayElemAt": ["$acharHifen", 1] }
            }        
          },
        }   
      },
      {
        "$addFields": {
          "inicioMais": { 
            "$cond": { 
              "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
              "then": { "$toInt": "$inicioAuxMais"}, 
              "else": 0
            }
          },
          "fimMais": {
            "$cond": { 
              "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
              "then": 99,
              "else": 0
            }
          },
          "inicioHifen": { 
            "$cond": { 
              "if": { "$lte": [ {"$strLenCP": "$inicioAuxHifen"}, 2 ] },
              "then": { "$toInt": "$inicioAuxHifen"}, 
              "else": 0
            }
          },
          "fimHifen": {
            "$cond": { 
              "if": { "$lte": [ {"$strLenCP": "$fimAuxHifen"}, 2 ] },
              "then": { "$toInt": "$fimAuxHifen"}, 
              "else": 0
            }
          }      
        }
      },{
        "$match": {
          "$or": [              
            { "$and": [
              { "inicioMais": {"$lte": minimum}},
              { "fimMais": {"$gte": maximum}},
            ]},            
            { "$and": [
              { "inicioHifen": {"$gte": minimum}},
              { "fimHifen": {"$lte": maximum}},
            ]}
          ]
        }
      },      
      { '$skip': toSkip },
      { '$limit': limit }      
    ];
  
    return pipeline;  
  }
  // Todos resultados da stack entre os valores
  if (( city === '') && (stack !== '') && (initialYear !== '') && (finalYear !== '')) {    
    let minimum = Number(initialYear);
    let maximum = Number(finalYear);
    
    if (minimum > maximum) {
      const auxValue = maximum;
      maximum = minimum;
      minimum = auxValue;            
    }
    
    const pipeline = [
      {      
        "$match": {
          "technologies.name": {
            '$regex': new RegExp(stack, 'i')
          },            
        }      
      },
      {      
        "$addFields": {
          "aux": { "$split": ["$experience", " "]  }
        }   
      },
      {      
        "$addFields": {
          "primeiroFiltro": { "$arrayElemAt": ["$aux", 0] } 
        }   
      },
      {
        "$addFields": {
          "acharMais": { "$split": ["$primeiroFiltro", "+"]  },
          "acharHifen": { "$split": ["$primeiroFiltro", "-"]  },
        }   
      },
      {      
        "$addFields": {
          "inicioAuxMais": { "$arrayElemAt": ["$acharMais", 0] }, 
          "fimAuxMais": { 
            "$cond": {
              "if": { "$eq": [{"$arrayElemAt": ["$acharMais",0]}, "$primeiroFiltro"]},
              "then": "$primeiroFiltro",
              "else": { "$arrayElemAt": ["$acharMais", 1] }
            }        
          },
          "inicioAuxHifen": { "$arrayElemAt": ["$acharHifen", 0] }, 
          "fimAuxHifen": { 
            "$cond": {
              "if": { "$eq": [{"$arrayElemAt": ["$acharHifen",0]}, "$primeiroFiltro"]},
              "then": "$primeiroFiltro",
              "else": { "$arrayElemAt": ["$acharHifen", 1] }
            }        
          },
        }   
      },
      {
        "$addFields": {
          "inicioMais": { 
            "$cond": { 
              "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
              "then": { "$toInt": "$inicioAuxMais"}, 
              "else": 0
            }
          },
          "fimMais": {
            "$cond": { 
              "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
              "then": 99,
              "else": 0
            }
          },
          "inicioHifen": { 
            "$cond": { 
              "if": { "$lte": [ {"$strLenCP": "$inicioAuxHifen"}, 2 ] },
              "then": { "$toInt": "$inicioAuxHifen"}, 
              "else": 0
            }
          },
          "fimHifen": {
            "$cond": { 
              "if": { "$lte": [ {"$strLenCP": "$fimAuxHifen"}, 2 ] },
              "then": { "$toInt": "$fimAuxHifen"}, 
              "else": 0
            }
          }      
        }
      },{
        "$match": {
          "$or": [              
            { "$and": [
              { "inicioMais": {"$lte": minimum}},
              { "fimMais": {"$gte": maximum}},
            ]},            
            { "$and": [
              { "inicioHifen": {"$gte": minimum}},
              { "fimHifen": {"$lte": maximum}},
            ]}
          ]
        }
      },      
      { '$skip': toSkip },
      { '$limit': limit }      
    ];
  
    return pipeline;  
  }

  // Todos resultados da localidade entre os valores  
  if (( city !== '') && (stack === '') && (initialYear !== '') && (finalYear !== '')) {    
    let minimum = Number(initialYear);
    let maximum = Number(finalYear);
    
    if (minimum > maximum) {
      const auxValue = maximum;
      maximum = minimum;
      minimum = auxValue;            
    }
    
    const pipeline = [
      {
        "$match": {
          "city": {
            '$regex': new RegExp(city, 'i')
          }
        }
      },
      {      
        "$addFields": {
          "aux": { "$split": ["$experience", " "]  }
        }   
      },
      {      
        "$addFields": {
          "primeiroFiltro": { "$arrayElemAt": ["$aux", 0] } 
        }   
      },
      {
        "$addFields": {
          "acharMais": { "$split": ["$primeiroFiltro", "+"]  },
          "acharHifen": { "$split": ["$primeiroFiltro", "-"]  },
        }   
      },
      {      
        "$addFields": {
          "inicioAuxMais": { "$arrayElemAt": ["$acharMais", 0] }, 
          "fimAuxMais": { 
            "$cond": {
              "if": { "$eq": [{"$arrayElemAt": ["$acharMais",0]}, "$primeiroFiltro"]},
              "then": "$primeiroFiltro",
              "else": { "$arrayElemAt": ["$acharMais", 1] }
            }        
          },
          "inicioAuxHifen": { "$arrayElemAt": ["$acharHifen", 0] }, 
          "fimAuxHifen": { 
            "$cond": {
              "if": { "$eq": [{"$arrayElemAt": ["$acharHifen",0]}, "$primeiroFiltro"]},
              "then": "$primeiroFiltro",
              "else": { "$arrayElemAt": ["$acharHifen", 1] }
            }        
          },
        }   
      },
      {
        "$addFields": {
          "inicioMais": { 
            "$cond": { 
              "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
              "then": { "$toInt": "$inicioAuxMais"}, 
              "else": 0
            }
          },
          "fimMais": {
            "$cond": { 
              "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
              "then": 99,
              "else": 0
            }
          },
          "inicioHifen": { 
            "$cond": { 
              "if": { "$lte": [ {"$strLenCP": "$inicioAuxHifen"}, 2 ] },
              "then": { "$toInt": "$inicioAuxHifen"}, 
              "else": 0
            }
          },
          "fimHifen": {
            "$cond": { 
              "if": { "$lte": [ {"$strLenCP": "$fimAuxHifen"}, 2 ] },
              "then": { "$toInt": "$fimAuxHifen"}, 
              "else": 0
            }
          }      
        }
      },{
        "$match": {
          "$or": [              
            { "$and": [
              { "inicioMais": {"$lte": minimum}},
              { "fimMais": {"$gte": maximum}},
            ]},            
            { "$and": [
              { "inicioHifen": {"$gte": minimum}},
              { "fimHifen": {"$lte": maximum}},
            ]}
          ]
        }
      },      
      { '$skip': toSkip },
      { '$limit': limit }      
    ];
  
    return pipeline;  
  }  

}


const getLimitPipeline = (filters, toSkip, limit) => {
  const { 
    city, 
    stack, 
    initialYear, 
    type
  } = filters;
  const limitValue = Number(initialYear);

  if(type === 'até') {
    // Todos os candidatos com a Stack e Localidade até
    if(( city !== '') && (stack !== '')) {
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
            "aux": { "$split": ["$experience", " "]  }
          }   
        },
        {      
          "$addFields": {
            "primeiroFiltro": { "$arrayElemAt": ["$aux", 0] } 
          }   
        },
        {
          "$addFields": {
            "acharMais": { "$split": ["$primeiroFiltro", "+"]  },
            "acharHifen": { "$split": ["$primeiroFiltro", "-"]  },
          }   
        },
        {      
          "$addFields": {
            "inicioAuxMais": { "$arrayElemAt": ["$acharMais", 0] }, 
            "fimAuxMais": { 
              "$cond": {
                "if": { "$eq": [{"$arrayElemAt": ["$acharMais",0]}, "$primeiroFiltro"]},
                "then": "$primeiroFiltro",
                "else": { "$arrayElemAt": ["$acharMais", 1] }
              }        
            },
            "inicioAuxHifen": { "$arrayElemAt": ["$acharHifen", 0] }, 
            "fimAuxHifen": { 
              "$cond": {
                "if": { "$eq": [{"$arrayElemAt": ["$acharHifen",0]}, "$primeiroFiltro"]},
                "then": "$primeiroFiltro",
                "else": { "$arrayElemAt": ["$acharHifen", 1] }
              }        
            },
          }   
        },
        {
          "$addFields": {
            "inicioMais": { 
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
                "then": { "$toInt": "$inicioAuxMais"}, 
                "else": 0
              }
            },
            "fimMais": {
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
                "then": 99,
                "else": 0
              }
            },
            "inicioHifen": { 
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxHifen"}, 2 ] },
                "then": { "$toInt": "$inicioAuxHifen"}, 
                "else": 0
              }
            },
            "fimHifen": {
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$fimAuxHifen"}, 2 ] },
                "then": { "$toInt": "$fimAuxHifen"}, 
                "else": 0
              }
            }      
          }
        },{
          "$match": {
            "$and": [              
              { "inicioMais": {"$lte": limitValue}},
              { "fimHifen": {"$lte": limitValue}},
            ]
          }
        },
        { '$skip': toSkip },
        { '$limit': limit }        
      ];

      return pipeline;      
    }    

    // Todos os candidatos com a Stack até
    if((city === '') && (stack !== '')){      
      const pipeline = [
        {
          "$match": {
            "technologies.name": {
              "$regex": new RegExp(stack, 'i')
            }
          }
        },        
        {      
          "$addFields": {
            "aux": { "$split": ["$experience", " "]  }
          }   
        },
        {      
          "$addFields": {
            "primeiroFiltro": { "$arrayElemAt": ["$aux", 0] } 
          }   
        },
        {
          "$addFields": {
            "acharMais": { "$split": ["$primeiroFiltro", "+"]  },
            "acharHifen": { "$split": ["$primeiroFiltro", "-"]  },
          }   
        },
        {      
          "$addFields": {
            "inicioAuxMais": { "$arrayElemAt": ["$acharMais", 0] }, 
            "fimAuxMais": { 
              "$cond": {
                "if": { "$eq": [{"$arrayElemAt": ["$acharMais",0]}, "$primeiroFiltro"]},
                "then": "$primeiroFiltro",
                "else": { "$arrayElemAt": ["$acharMais", 1] }
              }        
            },
            "inicioAuxHifen": { "$arrayElemAt": ["$acharHifen", 0] }, 
            "fimAuxHifen": { 
              "$cond": {
                "if": { "$eq": [{"$arrayElemAt": ["$acharHifen",0]}, "$primeiroFiltro"]},
                "then": "$primeiroFiltro",
                "else": { "$arrayElemAt": ["$acharHifen", 1] }
              }        
            },
          }   
        },
        {
          "$addFields": {
            "inicioMais": { 
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
                "then": { "$toInt": "$inicioAuxMais"}, 
                "else": 0
              }
            },
            "fimMais": {
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
                "then": 99,
                "else": 0
              }
            },
            "inicioHifen": { 
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxHifen"}, 2 ] },
                "then": { "$toInt": "$inicioAuxHifen"}, 
                "else": 0
              }
            },
            "fimHifen": {
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$fimAuxHifen"}, 2 ] },
                "then": { "$toInt": "$fimAuxHifen"}, 
                "else": 0
              }
            }      
          }
        },{
          "$match": {
            "$and": [              
              { "inicioMais": {"$lte": limitValue}},
              { "fimHifen": {"$lte": limitValue}},
            ]
          }
        },
        { '$skip': toSkip },
        { '$limit': limit }        
      ];

      return pipeline;
    }

    // Todos os candidatos da Localidade até
    if(( city !== '') && (stack === '')) {
      const pipeline = [
        {
          "$match": {     
            "city": {
              '$regex': new RegExp(city, 'i')
            },          
          }
        },         
        {      
          "$addFields": {
            "aux": { "$split": ["$experience", " "]  }
          }   
        },
        {      
          "$addFields": {
            "primeiroFiltro": { "$arrayElemAt": ["$aux", 0] } 
          }   
        },
        {
          "$addFields": {
            "acharMais": { "$split": ["$primeiroFiltro", "+"]  },
            "acharHifen": { "$split": ["$primeiroFiltro", "-"]  },
          }   
        },
        {      
          "$addFields": {
            "inicioAuxMais": { "$arrayElemAt": ["$acharMais", 0] }, 
            "fimAuxMais": { 
              "$cond": {
                "if": { "$eq": [{"$arrayElemAt": ["$acharMais",0]}, "$primeiroFiltro"]},
                "then": "$primeiroFiltro",
                "else": { "$arrayElemAt": ["$acharMais", 1] }
              }        
            },
            "inicioAuxHifen": { "$arrayElemAt": ["$acharHifen", 0] }, 
            "fimAuxHifen": { 
              "$cond": {
                "if": { "$eq": [{"$arrayElemAt": ["$acharHifen",0]}, "$primeiroFiltro"]},
                "then": "$primeiroFiltro",
                "else": { "$arrayElemAt": ["$acharHifen", 1] }
              }        
            },
          }   
        },
        {
          "$addFields": {
            "inicioMais": { 
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
                "then": { "$toInt": "$inicioAuxMais"}, 
                "else": 0
              }
            },
            "fimMais": {
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
                "then": 99,
                "else": 0
              }
            },
            "inicioHifen": { 
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxHifen"}, 2 ] },
                "then": { "$toInt": "$inicioAuxHifen"}, 
                "else": 0
              }
            },
            "fimHifen": {
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$fimAuxHifen"}, 2 ] },
                "then": { "$toInt": "$fimAuxHifen"}, 
                "else": 0
              }
            }      
          }
        },{
          "$match": {
            "$and": [              
              { "inicioMais": {"$lte": limitValue}},
              { "fimHifen": {"$lte": limitValue}},
            ]
          }
        },
        { '$skip': toSkip },
        { '$limit': limit }        
      ];

      return pipeline;      
    }       

    // Todos os candidatos até 
    if(( city === '') && (stack === '')) {      
      const pipeline = [        
        {      
          "$addFields": {
            "aux": { "$split": ["$experience", " "]  }
          }   
        },
        {      
          "$addFields": {
            "primeiroFiltro": { "$arrayElemAt": ["$aux", 0] } 
          }   
        },
        {
          "$addFields": {
            "acharMais": { "$split": ["$primeiroFiltro", "+"]  },
            "acharHifen": { "$split": ["$primeiroFiltro", "-"]  },
          }   
        },
        {      
          "$addFields": {
            "inicioAuxMais": { "$arrayElemAt": ["$acharMais", 0] }, 
            "fimAuxMais": { 
              "$cond": {
                "if": { "$eq": [{"$arrayElemAt": ["$acharMais",0]}, "$primeiroFiltro"]},
                "then": "$primeiroFiltro",
                "else": { "$arrayElemAt": ["$acharMais", 1] }
              }        
            },
            "inicioAuxHifen": { "$arrayElemAt": ["$acharHifen", 0] }, 
            "fimAuxHifen": { 
              "$cond": {
                "if": { "$eq": [{"$arrayElemAt": ["$acharHifen",0]}, "$primeiroFiltro"]},
                "then": "$primeiroFiltro",
                "else": { "$arrayElemAt": ["$acharHifen", 1] }
              }        
            },
          }   
        },
        {
          "$addFields": {
            "inicioMais": { 
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
                "then": { "$toInt": "$inicioAuxMais"}, 
                "else": 0
              }
            },
            "fimMais": {
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
                "then": 99,
                "else": 0
              }
            },
            "inicioHifen": { 
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxHifen"}, 2 ] },
                "then": { "$toInt": "$inicioAuxHifen"}, 
                "else": 0
              }
            },
            "fimHifen": {
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$fimAuxHifen"}, 2 ] },
                "then": { "$toInt": "$fimAuxHifen"}, 
                "else": 0
              }
            }      
          }
        },{
          "$match": {
            "$and": [              
              { "inicioMais": {"$lte": limitValue}},
              { "fimHifen": {"$lte": limitValue}},
            ]
          }
        },
        { '$skip': toSkip },
        { '$limit': limit }        
      ];

      return pipeline;
    }
  } else if ( type === 'acima de') {
    // Todos os candidatos com a Stack e Localidade acima de
    if(( city !== '') && (stack !== '')) {
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
            "aux": { "$split": ["$experience", " "]  }
          }   
        },
        {      
          "$addFields": {
            "primeiroFiltro": { "$arrayElemAt": ["$aux", 0] } 
          }   
        },
        {
          "$addFields": {
            "acharMais": { "$split": ["$primeiroFiltro", "+"]  },
            "acharHifen": { "$split": ["$primeiroFiltro", "-"]  },
          }   
        },
        {      
          "$addFields": {
            "inicioAuxMais": { "$arrayElemAt": ["$acharMais", 0] }, 
            "fimAuxMais": { 
              "$cond": {
                "if": { "$eq": [{"$arrayElemAt": ["$acharMais",0]}, "$primeiroFiltro"]},
                "then": "$primeiroFiltro",
                "else": { "$arrayElemAt": ["$acharMais", 1] }
              }        
            },
            "inicioAuxHifen": { "$arrayElemAt": ["$acharHifen", 0] }, 
            "fimAuxHifen": { 
              "$cond": {
                "if": { "$eq": [{"$arrayElemAt": ["$acharHifen",0]}, "$primeiroFiltro"]},
                "then": "$primeiroFiltro",
                "else": { "$arrayElemAt": ["$acharHifen", 1] }
              }        
            },
          }   
        },
        {
          "$addFields": {
            "inicioMais": { 
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
                "then": { "$toInt": "$inicioAuxMais"}, 
                "else": 0
              }
            },
            "fimMais": {
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
                "then": 99,
                "else": 0
              }
            },
            "inicioHifen": { 
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxHifen"}, 2 ] },
                "then": { "$toInt": "$inicioAuxHifen"}, 
                "else": 0
              }
            },
            "fimHifen": {
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$fimAuxHifen"}, 2 ] },
                "then": { "$toInt": "$fimAuxHifen"}, 
                "else": 0
              }
            }      
          }
        },{
          "$match": {
            "$or": [
              { "inicioMais": {"$gte": limitValue}},
              { "inicioHifen": {"$gte": limitValue}},
            ]
          }
        },
        { '$skip': toSkip },
        { '$limit': limit }        
      ];

      return pipeline;      
    }    

    // Todos os candidatos com a Stack acima de
    if((city === '') && (stack !== '')){      
      const pipeline = [
        {
          "$match": {
            "technologies.name": {
              "$regex": new RegExp(stack, 'i')
            }
          }
        },        
        {      
          "$addFields": {
            "aux": { "$split": ["$experience", " "]  }
          }   
        },
        {      
          "$addFields": {
            "primeiroFiltro": { "$arrayElemAt": ["$aux", 0] } 
          }   
        },
        {
          "$addFields": {
            "acharMais": { "$split": ["$primeiroFiltro", "+"]  },
            "acharHifen": { "$split": ["$primeiroFiltro", "-"]  },
          }   
        },
        {      
          "$addFields": {
            "inicioAuxMais": { "$arrayElemAt": ["$acharMais", 0] }, 
            "fimAuxMais": { 
              "$cond": {
                "if": { "$eq": [{"$arrayElemAt": ["$acharMais",0]}, "$primeiroFiltro"]},
                "then": "$primeiroFiltro",
                "else": { "$arrayElemAt": ["$acharMais", 1] }
              }        
            },
            "inicioAuxHifen": { "$arrayElemAt": ["$acharHifen", 0] }, 
            "fimAuxHifen": { 
              "$cond": {
                "if": { "$eq": [{"$arrayElemAt": ["$acharHifen",0]}, "$primeiroFiltro"]},
                "then": "$primeiroFiltro",
                "else": { "$arrayElemAt": ["$acharHifen", 1] }
              }        
            },
          }   
        },
        {
          "$addFields": {
            "inicioMais": { 
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
                "then": { "$toInt": "$inicioAuxMais"}, 
                "else": 0
              }
            },
            "fimMais": {
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
                "then": 99,
                "else": 0
              }
            },
            "inicioHifen": { 
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxHifen"}, 2 ] },
                "then": { "$toInt": "$inicioAuxHifen"}, 
                "else": 0
              }
            },
            "fimHifen": {
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$fimAuxHifen"}, 2 ] },
                "then": { "$toInt": "$fimAuxHifen"}, 
                "else": 0
              }
            }      
          }
        },{
          "$match": {
            "$or": [
              { "inicioMais": {"$gte": limitValue}},
              { "inicioHifen": {"$gte": limitValue}},
            ]
          }
        },
        { '$skip': toSkip },
        { '$limit': limit }        
      ];

      return pipeline;
    }

    // Todos os candidatos da Localidade acima de
    if(( city === '') && (stack !== '')) {
      const pipeline = [
        {
          "$match": {     
            "technologies.name": {
              "$regex": new RegExp(stack, 'i')
            }
          }
        },         
        {      
          "$addFields": {
            "aux": { "$split": ["$experience", " "]  }
          }   
        },
        {      
          "$addFields": {
            "primeiroFiltro": { "$arrayElemAt": ["$aux", 0] } 
          }   
        },
        {
          "$addFields": {
            "acharMais": { "$split": ["$primeiroFiltro", "+"]  },
            "acharHifen": { "$split": ["$primeiroFiltro", "-"]  },
          }   
        },
        {      
          "$addFields": {
            "inicioAuxMais": { "$arrayElemAt": ["$acharMais", 0] }, 
            "fimAuxMais": { 
              "$cond": {
                "if": { "$eq": [{"$arrayElemAt": ["$acharMais",0]}, "$primeiroFiltro"]},
                "then": "$primeiroFiltro",
                "else": { "$arrayElemAt": ["$acharMais", 1] }
              }        
            },
            "inicioAuxHifen": { "$arrayElemAt": ["$acharHifen", 0] }, 
            "fimAuxHifen": { 
              "$cond": {
                "if": { "$eq": [{"$arrayElemAt": ["$acharHifen",0]}, "$primeiroFiltro"]},
                "then": "$primeiroFiltro",
                "else": { "$arrayElemAt": ["$acharHifen", 1] }
              }        
            },
          }   
        },
        {
          "$addFields": {
            "inicioMais": { 
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
                "then": { "$toInt": "$inicioAuxMais"}, 
                "else": 0
              }
            },
            "fimMais": {
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
                "then": 99,
                "else": 0
              }
            },
            "inicioHifen": { 
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxHifen"}, 2 ] },
                "then": { "$toInt": "$inicioAuxHifen"}, 
                "else": 0
              }
            },
            "fimHifen": {
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$fimAuxHifen"}, 2 ] },
                "then": { "$toInt": "$fimAuxHifen"}, 
                "else": 0
              }
            }      
          }
        },{
          "$match": {
            "$or": [
              { "inicioMais": {"$gte": limitValue}},
              { "inicioHifen": {"$gte": limitValue}},
            ]
          }
        },
        { '$skip': toSkip },
        { '$limit': limit }        
      ];

      return pipeline;      
    }       

    // Todos os candidatos acima de
    if(( city === '') && (stack === '')) {
      const pipeline = [        
        {      
          "$addFields": {
            "aux": { "$split": ["$experience", " "]  }
          }   
        },
        {      
          "$addFields": {
            "primeiroFiltro": { "$arrayElemAt": ["$aux", 0] } 
          }   
        },
        {
          "$addFields": {
            "acharMais": { "$split": ["$primeiroFiltro", "+"]  },
            "acharHifen": { "$split": ["$primeiroFiltro", "-"]  },
          }   
        },
        {      
          "$addFields": {
            "inicioAuxMais": { "$arrayElemAt": ["$acharMais", 0] }, 
            "fimAuxMais": { 
              "$cond": {
                "if": { "$eq": [{"$arrayElemAt": ["$acharMais",0]}, "$primeiroFiltro"]},
                "then": "$primeiroFiltro",
                "else": { "$arrayElemAt": ["$acharMais", 1] }
              }        
            },
            "inicioAuxHifen": { "$arrayElemAt": ["$acharHifen", 0] }, 
            "fimAuxHifen": { 
              "$cond": {
                "if": { "$eq": [{"$arrayElemAt": ["$acharHifen",0]}, "$primeiroFiltro"]},
                "then": "$primeiroFiltro",
                "else": { "$arrayElemAt": ["$acharHifen", 1] }
              }        
            },
          }   
        },
        {
          "$addFields": {
            "inicioMais": { 
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
                "then": { "$toInt": "$inicioAuxMais"}, 
                "else": 0
              }
            },
            "fimMais": {
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxMais"}, 2 ] },
                "then": 99,
                "else": 0
              }
            },
            "inicioHifen": { 
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$inicioAuxHifen"}, 2 ] },
                "then": { "$toInt": "$inicioAuxHifen"}, 
                "else": 0
              }
            },
            "fimHifen": {
              "$cond": { 
                "if": { "$lte": [ {"$strLenCP": "$fimAuxHifen"}, 2 ] },
                "then": { "$toInt": "$fimAuxHifen"}, 
                "else": 0
              }
            }      
          }
        },{
          "$match": {
            "$or": [
              { "inicioMais": {"$gte": limitValue}},
              { "inicioHifen": {"$gte": limitValue}},
            ]
          }
        },
        { '$skip': toSkip },
        { '$limit': limit }        
      ];

      return pipeline;
    }
  }
}

const getWithoutPeriodsPipeline = (filters, toSkip, limit) => {
  const {
    city,
    stack    
  } = filters;

  // Todos os candidatos
  if (( city === '') && (stack === '')) {
    const pipeline = [
      { '$skip': toSkip },
      { '$limit': limit }
    ];

    return pipeline;
  }

  // Todos os candidatos da Localidades e Stack
  if (( city !== '') && (stack !== '')) {
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

  if ((( city !== '') || (stack !== ''))) {
    // Todos os candidatos da Localidade
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
    // Todos os candidatos da Stack
    if( city === '') {
      const pipeline = [
        {
          "$match": {
            "technologies.name": {
              '$regex': new RegExp(stack, 'i')
            },            
          }
        },
        { '$skip': toSkip },
        { '$limit': limit }
      ];
  
      return pipeline;  
    }
  }

}

const matchCandidates = async (filters) => {  
  const matches = [];
  const { initialYear, finalYear, page } = filters;
  const limit = 5;
  const toSkip = (limit * page) - limit;
  const { type } = filters;
  let pipeline = [];

  if ((initialYear === '') && (finalYear === '')) {
    pipeline = getWithoutPeriodsPipeline(filters, toSkip, limit);
  } else {
    if (type === 'entre') {
      pipeline = getIntervalPipeline(filters, toSkip, limit);
    } else {
      pipeline = getLimitPipeline(filters, toSkip, limit);    
    }    
  }
  
  // db.candidates.aggregate([{$match: {city: {$regex: /^rio/i}}}]).pretty();
  
  //console.log(`pipe: ${pipeline}`);
  const result = await connection()
    .then((db) => db.collection('candidates').aggregate(pipeline).limit(limit))
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