const _ = require('lodash');
const Promise = require('bluebird');
const moment = require('moment');
const cassandra = require('cassandra-driver');
const async = require('async');
const elasticsearch = require('elasticsearch');


let cassandraClient = new cassandra.Client({contactPoints: ['127.0.0.1']});
let elasticClient = new elasticsearch.Client({host: 'localhost:9200',log: 'trace'});

  

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
  
  // elasticClient.search({
  //     q: 'int'
  //     }).then(function (body) {
  //     var hits = body.hits.total;
  //     console.log(hits);
  //     }, function (error) {
  //     console.trace(error.message);
  //     });
// let createcassandraelasticClient = () => {
//     var cassandraelasticClient = new cassandra.cassandraelasticClient({contactPoints: ['127.0.0.1'], keyspace: 'events'});
// }




let config = {
    headers: {"Content-Type": "application/x-ndjson"}
};

let seededAlready = false;
// let toCount = 0;
// let createESTO = (arr) => {
    //     console.log('in the createESTO function');
    //     arr.forEach((event) => {
        //         // console.log('in the createESTO for each');
//         let curCreate = { "create" : { "_index" : "events2", "_type" : "event_fired2" , "_id":event.id} };
//         let curEvent = { "id" : event.id, "listing_id": event.listing_id, "time": event.time , "type": event.type, "metadata": event.metadata.photo };
//         elasticSearchQueue.push(curCreate);
//         elasticSearchQueue.push(curEvent);
//     })
//     // elasticSearchQueue += JSON.stringify(curCreate) + '\n' + JSON.stringify(curEvent) + '\n';
//     // elasticSearchQueue += `{ "create" : { "_index" : "events", "_type" : "event_fired"} }\n{ "id" : ${event.id}, "listing_id":${event.listing_id}, "time":${event.time} , "type":${event.type}, "metadata":${event.metadata.photo} }\n`
//     if (elasticSearchQueue.length === 500) {
    //         let bulkPost = {body: elasticSearchQueue};
    //         // axios.post('http://localhost:9200/events/event_fired/_bulk', bulkPost, config);
//         // setTimeout(() =>{
    //         return new Promise((resolve,reject) => {elasticClient.bulk(bulkPost, (err,resp) => {
        //             if(err){return console.log('Error Seeding ES Bulk Batch :', err)}
        //             // console.log('Bulk Response :', resp);
        //             console.log('in the createESTO seed PROMISE');
        //             resolve();
        //         });
        //         elasticSearchQueue = [];
        //     });
        //         // }, 1000 * toCount)
        //         // toCount ++;
        //     }
        //     // setTimeout(() => {
            //     //     axios.post(`http://localhost:9200/events/event_fired/${event.id}`,{
                //     //         id: event.id,
                //     //         listing_id: event.listing_id,
                //     //         time: event.time,
                //     //         type: event.type,
//     //         metadata: event.metadata.photo
//     //     }, config
//     //     ).then(() => {
    //     //         console.log('i in nested setTimeout', i);
//     //     }).catch((err) =>{
    //     //         console.log('ERROR SEEDING ELASTIC SEARCH :', err);
//     //     })
//     // }, 20 * i);
// }


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
    console.log('in the seed batch cassandra function');
    return cassandraClient.batch(arr, {prepare:true});
}

let esQueue = [];
let seedBatchElastic = (arr) => {
    console.log('in the seed batch elastic function');
    // return new Promise((resolve,reject) => {
        //     createESTO(arr).then(() => {
            //         resolve();
    //     })
    // })
    // return createESTO(arr);
    esQueue = [];
    console.log('arr.length in seedBatchElastic', arr.length)
    // console.log('in the createESTO function');
    arr.forEach((event) => {
        // console.log('in the createESTO for each');
        let curCreate = { "create" : { "_index" : "events2", "_type" : "event_fired2" , "_id":event.id} };
        let curEvent = { "id" : event.id, "listing_id": event.listing_id, "time": event.time , "type": event.type, "metadata": event.metadata.photo };
        esQueue.push(curCreate);
        esQueue.push(curEvent);
    })
    // elasticSearchQueue += JSON.stringify(curCreate) + '\n' + JSON.stringify(curEvent) + '\n';
    // elasticSearchQueue += `{ "create" : { "_index" : "events", "_type" : "event_fired"} }\n{ "id" : ${event.id}, "listing_id":${event.listing_id}, "time":${event.time} , "type":${event.type}, "metadata":${event.metadata.photo} }\n`
    console.log('elastic queue', esQueue);
    console.log('elastic queue length', esQueue.length);
    
    if (esQueue.length === 1000) {
        let bulkPost = {body: esQueue};
        // axios.post('http://localhost:9200/events/event_fired/_bulk', bulkPost, config);
        // setTimeout(() =>{
        // return new Promise((resolve,reject) => {elasticClient.bulk(bulkPost, (err,resp) => {
        //     if(err){return console.log('Error Seeding ES Bulk Batch :', err)}
        //     // console.log('Bulk Response :', resp);
        //     console.log('in the createESTO seed PROMISE');
        //     resolve();
        // });
        return elasticClient.bulk(bulkPost);
    // });
    
        // }, 1000 * toCount)
        // toCount ++;
    }
    
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
    seedBatchElastic:seedBatchElastic,
    seedBatchCassandra:seedBatchCassandra,
    createKeyspace:createKeyspace,
    createTable:createTable,
    insertEvent:insertEvent,
    SelectAllEvents:SelectAllEvents,
    describeKeyspaces,describeKeyspaces
}
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