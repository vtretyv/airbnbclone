const _ = require('lodash');
const Promise = require('bluebird');
const moment = require('moment');
const cassandra = require('cassandra-driver');
const async = require('async');
const elasticsearch = require('elasticsearch');
const axios = require('axios');

// let cassandraClient = new cassandra.Client({contactPoints: ['127.0.0.1']});
let cassandraClient = new cassandra.Client({contactPoints: ['54.153.123.244']});



cassandraClient.options.socketOptions.readTimeout = 300000;
console.log ('Socket Options :', cassandraClient.options.socketOptions);



let config = {
    // headers: {"Content-Type": "application/x-ndjson"}
    headers: {"Content-Type": "application/json"}
    
};


//CASSANDRA
let createKeyspace = () => {
    return new Promise((resolve,reject) => {
        cassandraClient.execute("CREATE KEYSPACE events WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'};", (err,result) => {
            if(err){return console.log('ERROR CREATING NAMESPACE')};
        resolve();
        
    });
})
}

let createTable = () => {
    return new Promise((resolve,reject) =>{
        cassandraClient.execute('CREATE TABLE IF NOT EXISTS events.events (id int PRIMARY KEY, listing_id int, time timestamp, type text, metadata text);', (err,result) =>{
            if (!err){
                // console.log("EVENT TABLE CREATED OR WAS ALREADY CREATED");
            } else {
                console.log("ERROR CREATING EVENT TABLE", err);
            }
            resolve();        
        })
    })
}

let insertEvent = (id,listing_id,time,type,metadata) => {
    return new Promise((resolve,reject) => {
        cassandraClient.execute(`INSERT INTO events.events (id,listing_id,time, type, metadata) VALUES (${id},${listing_id},'${time}','${type}','${metadata}')`, function (err, result) {
            if (!err){
                // console.log("EVENT INSERTED INTO THE CASSANDRA DB");
                resolve();
            } else {
                console.log('ERROR INSERTING INTO CASSANDRA DB', err);
            }
            // Run next function in series
            // callback(err, null);
        })
    })
    
}

let describeKeyspaces = () => {
    return new Promise((resolve,reject) => {
        cassandraClient.execute('SELECT keyspace_name FROM system_schema.keyspaces;', (err,data) => {
            if (err) {return console.log('ERROR DESCRIBING KEYSPACE:', err)};
            // console.log('KEYSPACES DESCRIBED:', data);
            resolve(data);
        })
        // resolve(cassandraClient.metadata.keyspaces);//('events'));
    })
}

let seedBatchCassandra = (arr) => {
    // console.log('in the seed batch cassandra function');
    return cassandraClient.batch(arr, {prepare:true});
}

let getCassandraCountInt = () => {
    return cassandraClient.execute("SELECT count(*) FROM events.events WHERE metadata = 'int' ALLOW FILTERING;", [], {autopage:true})
};
let getCassandraCountExt = () => {
    return cassandraClient.execute("SELECT count(*) FROM events.events WHERE metadata = 'ext' ALLOW FILTERING;")    
};

let SelectAllEvents = () => {
    new Promise((resolve,reject) => {
        resolve(cassandraClient.execute("SELECT * FROM events.events;"));
    }).then((data) =>{
        console.log(data);
    })
}

module.exports = {
    getCassandraCountExt:getCassandraCountExt,
    getCassandraCountInt:getCassandraCountInt,
    seedBatchCassandra:seedBatchCassandra,
    createKeyspace:createKeyspace,
    createTable:createTable,
    insertEvent:insertEvent,
    SelectAllEvents:SelectAllEvents,
    describeKeyspaces,describeKeyspaces
}
