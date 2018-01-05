const _ = require('lodash');
const Promise = require('bluebird');
const moment = require('moment');
const cassandra = require('cassandra-driver');
const async = require('async');
const elasticsearch = require('elasticsearch');
const axios = require('axios');

let cassandraClient = new cassandra.Client({contactPoints: ['127.0.0.1']});
let elasticClient = new elasticsearch.Client({host: 'localhost:9200',log: 'trace'});
cassandraClient.options.socketOptions.readTimeout = 300000;
console.log ('Socket Options :', cassandraClient.options.socketOptions);                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        

//ELASTIC SEARCH
elasticClient.ping({
      // ping usually has a 3000ms timeout
      requestTimeout: 1000
    }, function (error) {
      if (error) {
        console.trace('elasticsearch cluster is down!');
      } else {
        console.log('All is well');
      }
      });





let config = {
    // headers: {"Content-Type": "application/x-ndjson"}
    headers: {"Content-Type": "application/json"}
    
};

let ratioCount = 10;
let insertIntoElastic = (ratio) => {
    ratioCount++;  
    return elasticClient.index({
        index: 'event_service',
        // id: String(ratioCount),
        type: 'ratios',
        body: {
            "Ratio": ratio,
            "Time": new Date(),
        }
    })    
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
let esQueue = [];
let seedBatchElastic = (arr) => {
    console.log('in the seed batch elastic function');

    esQueue = [];
    console.log('arr.length in seedBatchElastic', arr.length)
    arr.forEach((event) => {
        let curCreate = { "create" : { "_index" : "events2", "_type" : "event_fired2" , "_id":event.id} };
        let curEvent = { "id" : event.id, "listing_id": event.listing_id, "time": event.time , "type": event.type, "metadata": event.metadata.photo };
        esQueue.push(curCreate);
        esQueue.push(curEvent);
    })
    console.log('elastic queue', esQueue);
    console.log('elastic queue length', esQueue.length);
    
    if (esQueue.length === 1000) {
        let bulkPost = {body: esQueue};
        return elasticClient.bulk(bulkPost);

    }
    
}

let SelectAllEvents = () => {
    new Promise((resolve,reject) => {
        resolve(cassandraClient.execute("SELECT * FROM events.events;"));
    }).then((data) =>{
        console.log(data);
    })
}

module.exports = {
    // InsertEvent:InsertEvent,
    // createcassandraClient,createcassandraClient,
    elasticClient:elasticClient,
    insertIntoElastic:insertIntoElastic,
    getCassandraCountExt:getCassandraCountExt,
    getCassandraCountInt:getCassandraCountInt,
    seedBatchElastic:seedBatchElastic,
    seedBatchCassandra:seedBatchCassandra,
    createKeyspace:createKeyspace,
    createTable:createTable,
    insertEvent:insertEvent,
    SelectAllEvents:SelectAllEvents,
    describeKeyspaces,describeKeyspaces
}
// let InsertEvent = (id,time,type,metadata) => {
//     return new Promise((resolve, reject) => {new Promise ((resolve,reject) => {
//         cassandraClient.execute("CREATE KEYSPACE IF NOT EXISTS events WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'};" );
//         resolve();
//     }).then(() => {
//     new Promise((resolve,reject) =>{
//         cassandraClient.execute('CREATE TABLE IF NOT EXISTS events.events (id int PRIMARY KEY,time timestamp, type text, metadata text);', (err,result) =>{
//             if (!err){
//                 // console.log("EVENT TABLE CREATED OR WAS ALREADY CREATED");
//             } else {
//                 console.log("ERROR CREATING EVENT TABLE", err);
//             }
//         })
//         resolve();        
//     }).then(() => {
//         cassandraClient.execute(`INSERT INTO Events.events (id, time, type, metadata) VALUES (${id},'${time}','${type}','${metadata}')`, function (err, result) {
//             if (!err){
//                 // console.log("EVENT INSERTED INTO THE CASSANDRA DB");
//             } else {
//                 console.log("ERROR INSERTING INTO CASSANDRA DB", err);
//             }
//             // Run next function in series
//             // callback(err, null);
//         });
//         }).catch((err) => {
//             console.log('ERROR CREATING KEYSPACE', err);
//         })
//     })
//     resolve();
// })
// }
// mongoose.connect('mongodb://localhost/test');
// "use strict";
// var db = mongoose.connection;

// db.on('error', function() {
//   console.log('mongoose connection error');
// });

// db.once('open', function() {
//   console.log('mongoose connected successfully');
// });

// var itemSchema = mongoose.Schema({
//   type: String,
//   target: String,
//   location: {type:String, default:null},
//   mediaClassification: {type:String, default:null},
//   date: Date,
//   primary: {type:Boolean, default:null}
// });

// var Event = mongoose.model('Event', itemSchema);

// var selectAll = ()=>{
//   return new Promise((resolve,reject)=>{
//     resolve(Item.find({}).limit(50));
//   })
// };

// let save = (items) => {
//   return new Promise((resolve,reject) => {
//   items.forEach((item)=>{
//     newItem = new Item({
//       type: item.type,
//       target: item.target,
//       location: item.location,
//       mediaClassification: item.mediaClassification,
//       date: item.date,
//       primary: item.primary
//     })
//     // Item.findOneAndUpdate({'type':newItem.type}, {target: newItem.target,description: newItem,fullVideoUrl: 'https://www.youtube.com/watch?v='+ item.id.videoId, videoUrl: item.id.videoId, channelTitle:item.snippet.channelId,publishedAt: item.snippet.publishedAt, thumbnail:item.snippet.thumbnails.default.url}, {upsert:true}, (err,doc)=>{
//     //   if(err) {throw err};
//     // })
//     newItem.save({'_id': item._id},(err,data) =>{
//       if (err) {throw err};
//     });
//   });
// })
// }

// module.exports = {
//   selectAll:selectAll,
//   save:save,
// }