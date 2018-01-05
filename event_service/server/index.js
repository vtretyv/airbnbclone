const express = require('express');
const session = require('express-session');
const bodyParser = require('body-parser');
const request = require('request');
const path = require('path');
const Promise = require('bluebird');
const axios = require('axios');
const fs = require('fs');

const cluster = require('cluster');
const http = require('http');
const numCPUs = require('os').cpus().length;

// const elasticsearch = require('elasticsearch');





const PORT = process.env.PORT || 3000;
const moment = require('moment');
const loadtest = require('loadtest');

const apm = require('elastic-apm-node').start({
    appName: 'kibana-metrics',
})
const app = express();
// const app2 = express();  
// const app3 = express();
// const app4 = express();
// const app5 = express();




app.use(express.static(path.join(__dirname, '/../client/dist')));
app.use(bodyParser.json());
// app2.use(bodyParser.json());
// app3.use(bodyParser.json());
// app4.use(bodyParser.json());
// app5.use(bodyParser.json());

app.use(apm.middleware.express())


// ======================================================================
//        Database Functions
// ======================================================================
const db = require('../database/index.js');
const dataGen = require('../sampleData/dataGen.js');





console.log('New date: ',new Date());

let config = {  
    headers: {"Content-Type": "application/x-ndjson"}
};

let seededAlready = false;
let elasticSearchQueue = [];
let toCount = 0;

// ======================================================================
//        Initialization of DB
// ======================================================================
db.describeKeyspaces()
.then((data) => {
    // console.log('KEYSPACE DATA: ', data.rows);
    for (let i = 0; i < data.rows.length; i ++) {
        if (data.rows[i].keyspace_name === 'events'){
            // console.log('true');
            seededAlready = true;
        }
    }
    console.log('SEEDED ALREADY?', seededAlready);
})
.then(() => {
    console.log('after seeding then');
    axios.get('http://localhost:9200/event_service/')
    .then((data) => {
        console.log('in axios get then');
        // console.log('Data from elastic get :', data);
        if (data.status === 200) {
            console.log('elastic index already created :D')
        } else {
            elasticClient.indices.create({
                index: 'event_service'
            })
            .then(() =>{
                console.log('index created');
            })
            .catch((err) => {
                console.log('failed to create index')
            })
        }
    })
    .catch((err) => {
        console.log('in axios get catch');
        // console.log('error creating elastic index :', err);
        db.elasticClient.indices.create({
            index: 'event_service'
        })
        .then(() =>{
            console.log('in the catchs index create then');            
            console.log('elastic index created');
        })
        .catch((err) => {
            console.log('failed to create elastic index :', err);
        })
    })
})
.then(() => {
    if (seededAlready === false) {
        db.createKeyspace().then(() => {
            console.log('Keyspace events created')

            //Cassandra Seeding
            db.createTable().then(() => {
                console.log('Created Events Table')
                let queries = [];
                let count = 0;
                let recurseCassandra = () => {
                    queries = [];
                    console.log('INSIDE recurseCassandra');
                    if (count === 10000000) {
                        return console.log('Count at :', count);
                    }
                    for (let j = 0; j < 500; j ++){   
                        count ++;
                        var event = dataGen.dataGenerator();
                        let query = 'INSERT INTO events.events (id,listing_id,time,type,metadata) VALUES (?,?,?,?,?);';
                        let params = [event.id,event.listing_id,event.time,event.type,event.metadata.photo];
                        let curQuer = {query: query, params:params};
                        queries.push(curQuer);
                        if (queries.length === 500){
                            db.seedBatchCassandra(queries)
                            .then(() => {
                                console.log(' CASSANDRA BATCH SEEDED TO CASSANDRA, HERE IS COUNT: ', count);
                                recurseCassandra();
                            })
                            .catch((err) => {
                                console.log('ERROR SEEDING BATCH TO CASSANDRA: ', err);
                            })
                        }
                }
            }
            recurseCassandra();
            //Elastic Search Seeding
            }).catch((err) => {
                console.log('Error creating events Table', err);
            })
        }).catch(() => {
            console.log('Error creating keyspace');
        })
    }
})
.then(() => {
    setInterval(() =>{        
    let ratio = 0;
    let intCount = 0;
    let extCount = 0;
    db.getCassandraCountInt()
    .then((data) => {
        // console.log('rawData :', data);
        intCount = data.rows[0].count.toNumber();
        
        console.log('Count of Interior Photos Selects :', intCount);
        db.getCassandraCountExt()
        .then((data) => {
            // console.log('rawData :', data);
            extCount = data.rows[0].count.toNumber();
            console.log('Count of Interior Photos Selects :', extCount);
        })
        .then(() => {
            //Elastic Search Ratio Post here
            ratio = intCount/extCount;
            console.log('ratio before elastic post :', ratio);
            // setInterval(() =>{
                db.insertIntoElastic(ratio)
                .then(() => {
                    // console.log('Inserted Ratio into Elastic');
                    console.log('Got info from elastic');
                    
                })
                .catch((err) => {
                    console.log('Error Inserting Ratio into Elastic :', err);
                })
            // },150000)
        })
        .catch((err) => {
            console.log('Error getting count of interior photos :', err);
        })
    })
    .catch((err) => {
        console.log('Error getting count of interior photos :', err);
    })
},150000)

})

let casQueue = [];


app.post('/events', (req, res) => {
    let newEvent = req.body;
    let query = 'INSERT INTO events.events (id,listing_id,time,type,metadata) VALUES (?,?,?,?,?);';
    let params = [newEvent.id,newEvent.listing_id,newEvent.time,newEvent.type,newEvent.metadata.photo];
    let curQuer = {query: query, params:params};
    casQueue.push(curQuer);
    // console.log('casQueue.length :', casQueue.length);
    if (casQueue.length < 100) {
        res.sendStatus(201);
    }
    if (casQueue.length === 100) {
        // res.sendStatus(201);        
        let curBatch = casQueue;
        casQueue = [];

        db.seedBatchCassandra(curBatch)
        .then(() => {
            dataGen.orderID += 100;
            console.log('BATCH OF 100 SEND TO CASSANDRA FROM POST ROUTE');
            // console.log('casQueue :', casQueue);
            res.sendStatus(201);
        })
        .catch((err) => {
            console.log('BATCH OF 100 failed to SEED INTO CASSANDRA');
        })
        // res.sendStatus(201);
    }

});

app.listen(PORT, ()=> {console.log(`Listening on Port ${PORT}`)} );



//Node Clustering for local RPS Testing
// if (cluster.isMaster) {
//     console.log(`Master ${process.pid} is running`);
  
//     // Fork workers.
//     for (let i = 0; i < numCPUs; i++) {
//       cluster.fork();
//       console.log('number of cpus :', i);
//     }
  
//     cluster.on('exit', (worker, code, signal) => {
//       console.log(`worker ${worker.process.pid} died`);
//     });
//   } else {
//     // Workers can share any TCP connection
//     // In this case it is an HTTP server
//     const express = require('express');
//     const app = express();
//     app.use(bodyParser.json());


//     app.post('/events', (req, res) => {
//         let newEvent = req.body;
//         let query = 'INSERT INTO events.events (id,listing_id,time,type,metadata) VALUES (?,?,?,?,?);';
//         let params = [newEvent.id,newEvent.listing_id,newEvent.time,newEvent.type,newEvent.metadata.photo];
//         let curQuer = {query: query, params:params};
//         casQueue.push(curQuer);
//         // console.log('casQueue.length :', casQueue.length);
//         if (casQueue.length < 100) {
//             res.sendStatus(201);
//         }
//         if (casQueue.length === 100) {
//             // res.sendStatus(201);        
//             let curBatch = casQueue;
//             casQueue = [];
    
//             db.seedBatchCassandra(curBatch)
//             .then(() => {
//                 dataGen.orderID += 100;
//                 console.log('BATCH OF 100 SEND TO CASSANDRA FROM POST ROUTE');
//                 // console.log('casQueue :', casQueue);
//                 res.sendStatus(201);
//             })
//             .catch((err) => {
//                 console.log('BATCH OF 100 failed to SEED INTO CASSANDRA');
//             })
//             // res.sendStatus(201);
//         }
//     });
//     app.listen(PORT, ()=> {console.log(`Listening on Port ${PORT}`)} );

//     console.log(`Worker ${process.pid} started`);
//   }


//NGINX instance setup
// app2.get('/', (req,res) => {
//     res.send('Ya deed it route 1');
// })
// app3.get('/', (req,res) => {
//     res.send('Ya deed it route 2');
// })
// app2.post('/events', (req, res) => {
//     let newEvent = req.body;
//     let query = 'INSERT INTO events.events (id,listing_id,time,type,metadata) VALUES (?,?,?,?,?);';
//     let params = [newEvent.id,newEvent.listing_id,newEvent.time,newEvent.type,newEvent.metadata.photo];
//     let curQuer = {query: query, params:params};
//     casQueue.push(curQuer);
//     // console.log('casQueue.length :', casQueue.length);
//     if (casQueue.length < 100) {
//         res.sendStatus(201);
//     }
//     if (casQueue.length === 100) {
//         // res.sendStatus(201);        
//         let curBatch = casQueue;
//         casQueue = [];

//         db.seedBatchCassandra(curBatch)
//         .then(() => {
//             dataGen.orderID += 100;
//             console.log('BATCH OF 100 SEND TO CASSANDRA FROM POST ROUTE');
//             // console.log('casQueue :', casQueue);
//             res.sendStatus(201);
//         })
//         .catch((err) => {
//             console.log('BATCH OF 100 failed to SEED INTO CASSANDRA');
//         })
//         // res.sendStatus(201);
//     }

// });

// app3.post('/events', (req, res) => {
//     let newEvent = req.body;
//     let query = 'INSERT INTO events.events (id,listing_id,time,type,metadata) VALUES (?,?,?,?,?);';
//     let params = [newEvent.id,newEvent.listing_id,newEvent.time,newEvent.type,newEvent.metadata.photo];
//     let curQuer = {query: query, params:params};
//     casQueue.push(curQuer);
//     // console.log('casQueue.length :', casQueue.length);
//     if (casQueue.length < 100) {
//         res.sendStatus(201);
//     }
//     if (casQueue.length === 100) {
//         // res.sendStatus(201);        
//         let curBatch = casQueue;
//         casQueue = [];

//         db.seedBatchCassandra(curBatch)
//         .then(() => {
//             dataGen.orderID += 100;
//             console.log('BATCH OF 100 SEND TO CASSANDRA FROM POST ROUTE');
//             // console.log('casQueue :', casQueue);
//             res.sendStatus(201);
//         })
//         .catch((err) => {
//             console.log('BATCH OF 100 failed to SEED INTO CASSANDRA');
//         })
//         // res.sendStatus(201);
//     }

// });

// app4.post('/events', (req, res) => {
//     let newEvent = req.body;
//     let query = 'INSERT INTO events.events (id,listing_id,time,type,metadata) VALUES (?,?,?,?,?);';
//     let params = [newEvent.id,newEvent.listing_id,newEvent.time,newEvent.type,newEvent.metadata.photo];
//     let curQuer = {query: query, params:params};
//     casQueue.push(curQuer);
//     // console.log('casQueue.length :', casQueue.length);
//     if (casQueue.length < 100) {
//         res.sendStatus(201);
//     }
//     if (casQueue.length === 100) {
//         // res.sendStatus(201);        
//         let curBatch = casQueue;
//         casQueue = [];

//         db.seedBatchCassandra(curBatch)
//         .then(() => {
//             dataGen.orderID += 100;
//             console.log('BATCH OF 100 SEND TO CASSANDRA FROM POST ROUTE');
//             // console.log('casQueue :', casQueue);
//             res.sendStatus(201);
//         })
//         .catch((err) => {
//             console.log('BATCH OF 100 failed to SEED INTO CASSANDRA');
//         })
//         // res.sendStatus(201);
//     }

// });

// app5.post('/events', (req, res) => {
//     let newEvent = req.body;
//     let query = 'INSERT INTO events.events (id,listing_id,time,type,metadata) VALUES (?,?,?,?,?);';
//     let params = [newEvent.id,newEvent.listing_id,newEvent.time,newEvent.type,newEvent.metadata.photo];
//     let curQuer = {query: query, params:params};
//     casQueue.push(curQuer);
//     // console.log('casQueue.length :', casQueue.length);
//     if (casQueue.length < 100) {
//         res.sendStatus(201);
//     }
//     if (casQueue.length === 100) {
//         // res.sendStatus(201);        
//         let curBatch = casQueue;
//         casQueue = [];

//         db.seedBatchCassandra(curBatch)
//         .then(() => {
//             dataGen.orderID += 100;
//             console.log('BATCH OF 100 SEND TO CASSANDRA FROM POST ROUTE');
//             // console.log('casQueue :', casQueue);
//             res.sendStatus(201);
//         })
//         .catch((err) => {
//             console.log('BATCH OF 100 failed to SEED INTO CASSANDRA');
//         })
//         // res.sendStatus(201);
//     }

// });
// app2.listen(3001, ()=> {console.log(`Listening on Port 3001`)} );
// app3.listen(3002, ()=> {console.log(`Listening on Port 3002`)} );
// app4.listen(3003, ()=> {console.log(`Listening on Port 3003`)} );
// app5.listen(3004, ()=> {console.log(`Listening on Port 3004`)} );





// ======================================================================
//        Loadtest Function
// ======================================================================

// const options = {
//     url: 'http://localhost:3000/events',
//     method: 'POST',
//     contentType: 'application/json',
//     maxSeconds: 10,
//     requestsPerSecond: 1500, 
//     concurrency: 200,
//     body: {id:5003,listing_id:1, time:'2017-12-04 02:40:27',type:'select',metadata:{photo:'int'}},
    // body: dataGen.dataGenerator(),
    // headers: {'Content-type':'application/json'},
// }
// loadtest.loadTest(options, function(err, result) {
//     if (err) {
//       console.log(`i got an error ${error}`)
//     } else {
//       console.log(`Tests run successfully here's result RPS: ${result}`)
//       fs.appendFile('data.js', `FINAL RESULT: ${JSON.stringify(result)} \n`, (err) => {
//         err ? console.log(err) : console.log('success!')
//       })
//     }
// })

//DEPRECATED
let createESTO = (event,i) => {
    let curCreate = { "create" : { "_index" : "events", "_type" : "event_fired" , "_id":event.id} };
    let curEvent = { "id" : event.id, "listing_id": event.listing_id, "time": event.time , "type": event.type, "metadata": event.metadata.photo };
    elasticSearchQueue.push(curCreate);
    elasticSearchQueue.push(curEvent);
    // elasticSearchQueue += JSON.stringify(curCreate) + '\n' + JSON.stringify(curEvent) + '\n';
    // elasticSearchQueue += `{ "create" : { "_index" : "events", "_type" : "event_fired"} }\n{ "id" : ${event.id}, "listing_id":${event.listing_id}, "time":${event.time} , "type":${event.type}, "metadata":${event.metadata.photo} }\n`
    if (elasticSearchQueue.length === 1000) {
        let bulkPost = {body: elasticSearchQueue};
        // axios.post('http://localhost:9200/events/event_fired/_bulk', bulkPost, config);
        // setTimeout(() =>{
        client.bulk(bulkPost, (err,resp) => {
            if(err){return console.log('Error Seeding ES Bulk Batch :', err)}
            // console.log('Bulk Response :', resp);
        });
        elasticSearchQueue = [];
        // }, 1000 * toCount)
        toCount ++;
    }
    // setTimeout(() => {
    //     axios.post(`http://localhost:9200/events/event_fired/${event.id}`,{
    //         id: event.id,
    //         listing_id: event.listing_id,
    //         time: event.time,
    //         type: event.type,
    //         metadata: event.metadata.photo
    //     }, config
    //     ).then(() => {
    //         console.log('i in nested setTimeout', i);
    //     }).catch((err) =>{
    //         console.log('ERROR SEEDING ELASTIC SEARCH :', err);
    //     })
    // }, 20 * i);
}
// db.describeKeyspaces().then((data) => {
//     // console.log('KEYSPACE DATA: ', data.rows);
//     for (let i = 0; i < data.rows.length; i ++) {
//         if (data.rows[i].keyspace_name === 'events'){
//             // console.log('true');
//             seededAlready = true;
//         }
//     }
//     console.log('SEEDED ALREADY?', seededAlready);
// }).then(() => {
//     if (seededAlready === false) {
//         db.createKeyspace().then(() => {
//             console.log('Keyspace events created')

//             //Cassandra Seeding
//             db.createTable().then(() => {
//                 console.log('Created Events Table')
//                 for (let j = 0; j < 10000; j ++){            
//                 setTimeout(()=>{for (let i = 0; i < 1000; i ++) {
//                     var event = dataGen.dataGenerator();
//                     // axios(`https://localhost:9200/events/event_fired/`);
//                     db.insertEvent(event.id, event.listing_id, event.time ,event.type,event.metadata.photo);
//                     // createESTO(event,i);
//                 }
//                 },200 * j)
//             }
//             //Elastic Search Seeding

//             }).catch((err) => {
//                 console.log('Error creating events Table', err);
//             })
//         }).catch(() => {
//             console.log('Error creating keyspace');
//         })
//     }
// })

// db.describeKeyspaces().then((data) => {
//     // console.log('KEYSPACE DATA: ', data.rows);
//     for (let i = 0; i < data.rows.length; i ++) {
//         if (data.rows[i].keyspace_name === 'events'){
//             // console.log('true');
//             seededAlready = true;
//         }
//     }
//     console.log('SEEDED ALREADY?', seededAlready);
// }).then(() => {
//     if (seededAlready === false) {
//         db.createKeyspace().then(() => {
//             console.log('Keyspace events created')

//             //Cassandra Seeding
//             db.createTable().then(() => {
//                 console.log('Created Events Table')
//                 let queries = [];
//                 let count = 0;
//                 let elasticEvents = [];
//                 let recurseCassandra = () => {
//                     queries = [];
//                     elasticEvents = [];
//                     console.log('INSIDE recurseCassandra');
//                     if (count === 10000000) {
//                         return console.log('Count at :', count);
//                     }
//                     for (let j = 0; j < 500; j ++){   
//                         count ++;
//                         var event = dataGen.dataGenerator();
//                         elasticEvents.push(event);
//                         let query = 'INSERT INTO events.events (id,listing_id,time,type,metadata) VALUES (?,?,?,?,?);';
//                         let params = [event.id,event.listing_id,event.time,event.type,event.metadata.photo];
//                         let curQuer = {query: query, params:params};
//                         queries.push(curQuer);
//                         if (queries.length === 500 && elasticEvents.length === 500){
//                                 db.seedBatchCassandra(queries)
//                             .then(() => {
//                                 console.log('in the seedBatchCassandra Then');
//                                 db.seedBatchElastic(elasticEvents)
//                                 .then(() => {
//                                     recurseCassandra();
//                                 })
//                                 .catch((err) => {
//                                     console.log('ERROR SEEDING BATCH TO ELASTIC :', err);
//                                 })
//                             })
//                             .then(() => {
//                                 console.log(' CASSANDRA BATCH SEEDED TO CASSANDRA, HERE IS COUNT: ', count);
//                                 // recurseCassandra();
//                             })
//                             .catch((err) => {
//                                 console.log('ERROR SEEDING BATCH TO CASSANDRA: ', err);
//                             })
//                         }
//                 }
                             
//                 // setTimeout(()=>{for (let i = 0; i < 1000; i ++) {
//                 //     var event = dataGen.dataGenerator();
//                 //     // axios(`https://localhost:9200/events/event_fired/`);
//                 //     db.insertEvent(event.id, event.listing_id, event.time ,event.type,event.metadata.photo);
//                 //     // createESTO(event,i);
//                 // }
//                 // },200 * j)
//             }
//             recurseCassandra();
//             //Elastic Search Seeding

//             }).catch((err) => {
//                 console.log('Error creating events Table', err);
//             })
//         }).catch(() => {
//             console.log('Error creating keyspace');
//         })
//     }
// })

// ======================================================================
//        Elastic Search Set Up
// ======================================================================
// let client = new elasticsearch.Client({
//   host: 'localhost:9200',
//   log: 'trace'
// });

// client.ping({
//     // ping usually has a 3000ms timeout
//     requestTimeout: 1000
//   }, function (error) {
//     if (error) {
//       console.trace('elasticsearch cluster is down!');
//     } else {
//       console.log('All is well');
//     }
//     });

// // client.search({
// //     q: 'int'
// //     }).then(function (body) {
// //     var hits = body.hits.total;
// //     console.log(hits);
// //     }, function (error) {
// //     console.trace(error.message);
// //     });