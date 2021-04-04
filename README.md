# Findev - backend

Foi utilizado o [MongoDB](https://www.mongodb.com/) para o backend da aplicação.<br>
## Preparando o Hello Findev
Clone o repositório e instale as dependências:
`git clone git@github.com:andreserudo/findev-backend.git`
`yarn install` ou `npm install`<br>
### Hello Findev
Para iniciar o servidor, execute: `yarn dev`
A aplicação estará rodando na porta 3002.
O endereço da aplicação é: `http://localhost:3002/candidates/`
### A estrutura
Será necessário ter um banco e coleção com o nome de `candidates`.
Cada item da coleção tem o seguinte formato:
````
  "id": number,
  "city": string,
  "experience": string,
  "technologies": [
    "name":" string,
    "is_main_tech": bool, 
  ]
```` 
### A API 
Dado o endereço da aplicação a API suporte três rotas e todas retornaram um array como retorno.
Exemplo: `http://localhost:3002/candidates/`

1. 'all': retorna toda a coleção;
2. 'new': dado um array de itens, retorna uma array com os itens que foram salvos na coleção.
3. 'match': dado a entrada, retorna um array com os items que satisfazem o filtro de entrada.<br>
A entrada:
````  
  "page": string,
  "city": string,
  "stack": string,
  "initialYear": string,
  "finalYear": string,
  "type": string que pode ser: "até", "entre" ou "acima de",  
```` 