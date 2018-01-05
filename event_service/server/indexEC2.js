const express = require('express');
const session = require('express-session');
const bodyParser = require('body-parser');
const request = require('request');
const path = require('path');
const Promise = require('bluebird');
const axios = require('axios');
const fs = require('fs');
const moment = require('moment');

const PORT = process.env.PORT || 3000;

const app = express();

app.use(express.static(path.join(__dirname, '/../client/dist')));
app.use(bodyParser.json());

// ======================================================================
//        Database Functions
// ======================================================================
const db = require('../database/indexEC2.js');
const dataGen = require('../sampleData/dataGen.js');

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
                    // console.log('INSIDE recurseCassandra');
                    if (count === 10000000) {
                        return console.log('Count at :', count);
                    }
                    // if (count % 100000 === 0) {
                    //     console.log('Current Count :', count);
                    // }
                    for (let j = 0; j < 500; j ++){   
                        count ++;
                        var event = dataGen.dataGenerator();
                        let query = 'INSERT INTO events.events (id,listing_id,time,type,metadata) VALUES (?,?,?,?,?);';
                        let params = [event.id,event.listing_id,event.time,event.type,event.metadata.photo];
                        let curQuer = {query: query, params:params};
                        queries.push(curQuer);
                        if (queries.length === 500){
                            // setTimeout(()=>{
                                db.seedBatchCassandra(queries)
                                .then(() => {
                                    console.log(' CASSANDRA BATCH SEEDED TO CASSANDRA, HERE IS COUNT: ', count);
                                    recurseCassandra();
                                })
                                .catch((err) => {
                                    console.log('ERROR SEEDING BATCH TO CASSANDRA: ', err);
                                    console.log('ERROR SEEDING BATCH TO CASSANDRA: ', err.innerErrors['127.0.0.1:9042'].innerError);
                                    
                                })
                            // },Math.floor(count/20000));
                            
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


let casQueue = [];

app.post('/events', (req, res) => {
    let newEvent = req.body;
    let query = 'INSERT INTO events.events (id,listing_id,time,type,metadata) VALUES (?,?,?,?,?);';
    let params = [newEvent.id,newEvent.listing_id,newEvent.time,newEvent.type,newEvent.metadata.photo];
    let curQuer = {query: query, params:params};
    casQueue.push(curQuer);
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
            res.sendStatus(201);
        })
        .catch((err) => {
            console.log('BATCH OF 100 failed to SEED INTO CASSANDRA');
        })
        // res.sendStatus(201);
    }

});

app.listen(PORT, ()=> {console.log(`Listening on Port ${PORT}`)} );

