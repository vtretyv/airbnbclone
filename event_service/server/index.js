const express = require('express');
const session = require('express-session');
const bodyParser = require('body-parser');
const request = require('request');
const path = require('path');
const Promise = require('bluebird');
const axios = require('axios');
// const elasticsearch = require('elasticsearch');


const PORT = process.env.PORT || 3000;
const moment = require('moment');
const loadtest = require('loadtest');


const app = express();


app.use(express.static(path.join(__dirname, '/../client/dist')));
app.use(bodyParser.json());
app.listen(PORT, ()=> {console.log(`Listening on Port ${PORT}`)} );

// ======================================================================
//        Database Functions
// ======================================================================
const db = require('../database/index.js');
const dataGen = require('../sampleData/dataGen.js');


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




let config = {
    headers: {"Content-Type": "application/x-ndjson"}
};

let seededAlready = false;
let elasticSearchQueue = [];
let toCount = 0;
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
//        Initialization of DB
// ======================================================================
db.describeKeyspaces().then((data) => {
    // console.log('KEYSPACE DATA: ', data.rows);
    for (let i = 0; i < data.rows.length; i ++) {
        if (data.rows[i].keyspace_name === 'events'){
            // console.log('true');
            seededAlready = true;
        }
    }
    console.log('SEEDED ALREADY?', seededAlready);
}).then(() => {
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

// ======================================================================
//        Loadtest Function
// ======================================================================

let initLoadTest = () => {
    
}
  
app.post('/events', (req, res) => {
    // let newEvents = JSON.parse(req.body.events);
    // Inserting into cassandra
    // newEvents.forEach((pListing) => {
    //     db.InsertEvent(pListing.listing_id, pListing.time, pListing.type, pListing.metadata.photo).then(() => {
    //         // db.SelectAllEvents();
    //     });
    // });

    // let newEvent = JSON.parse(req.body);
    let newEvent = req.body;
    db.insertEvent(dataGen.orderID,newEvent.listing_id, newEvent.time, newEvent.type, newEvent.metadata.photo).then(() => {
        // console.log('Event Inserted into Cassanddra DB');
        dataGen.orderID++;
        res.sendStatus(201);
    });
    //Inserting into Elastic Search
    // let newEvents = JSON.parse(req.body.events);
    // newEvents.forEach((pListing, iter) => {
    //     createESTO(plisting,iter);
    // });

});