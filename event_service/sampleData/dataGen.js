const fs = require('fs');

let myTypes = ['search', 'post', 'select'];

let randomType = () => {
    let randNum = Math.floor(Math.random() * myTypes.length);
    return myTypes[randNum];
};
//Generates a random type from an array of types given
let randomID =  () => {
    let randNum = Math.floor(Math.random() * 10000000);
    return randNum;
};

let orderID = 0;
let orderedID = () => {
    orderID++;
    return orderID;
}

//Generates a random date
let randomTime = (startMonth, endMonth, startDay, endDay, startHour, endHour, startMin, endMin, startSec, endSec) => {
    // let month = (startMonth + Math.random() * (endMonth - startMonth));
    let month = '11';
    let year = '2017';
    let day = (startDay + Math.floor(Math.random() * (endDay - startDay)));
    if (day.length === 1) {
        day = '0'+ day;
    }
    // console.log('start', startDay);
    // console.log('end', endDay);
    // console.log(Math.random() * (endDay - startDay));
    // console.log('Day', day);
    // let day = '30';
    // let date = new Date(Date.UTC(year, month, String(day)));
    let date = '';
    
    let hour = String(startHour + Math.floor(Math.random() * (endHour - startHour)));
    if (hour.length === 1) {
        hour = '0'+ hour;
    }
    let minute = String(startMin + Math.floor(Math.random()  * (endMin - startMin)));    
    if (minute.length === 1) {
        minute = '0'+ minute;
    }
    let second = String(startSec + Math.floor(Math.random()  * (endSec - startSec)));
    if (second.length === 1) {
        second = '0'+ second;
    }
    date = year + '-' + '12' + '-' + String(day) + ' ' + hour + ':' + minute + ':' + second + 'Z';
    // let date2 = new Date(date);
    // console.log(date2);
    // console.log('date', date);
    // console.log('hour', hour);
    // console.log('minute', minute);
    // console.log('second', second);
    // date.setHours(hour);
    // date.setMinutes(minute);
    // date.setSeconds(second);
    return date;
}

let count = 0;
let intCount = 0;
let extCount = 0;
let randomPhoto = () =>{
    let inOut = ['int', 'ext'];
    let randNum = Math.floor(Math.random() * 2);
    if (randNum === 1) {
        extCount ++;
    }
    if (randNum === 0) {
        intCount ++;
    }

    if (extCount + 1000000 > intCount) {
        randNum = 0;
        extCount--;
        intCount++;
    }
    let metaData = {
     photo:inOut[randNum]   
    }
    // console.log('extCount', extCount);
    // console.log('intCount', intCount);
    return metaData;
}




let dataGenerator = ()=>{
    let tempObject = {
        id: orderedID(),
        listing_id: randomID(),
        time: randomTime(11, 12, 1, 30, 0, 23, 0, 60, 0, 60),
        type: randomType(),
        metadata: {photo: 'none'},
    }
    if (tempObject.type === 'select') {
        tempObject.metadata = randomPhoto();
    }
    return tempObject;
}

// console.log(dataGenerator());

let oneMil = [];
let twoMil = [];
let threeMil = [];
let fourMil = [];
let fiveMil = [];
let sixMil = [];
let sevenMil = [];
let eightMil = [];
let nineMil = [];
let tenMil = [];

// let listings = [];
// for (let i = 0; i < 1000000; i ++) {
//     listings.push(dataGenerator());
// }
// oneMil = listings.slice(0, 1000000);
// twoMil = listings.slice(1000000, 2000000);
// threeMil = listings.slice(2000000, 3000000);
// fourMil = listings.slice(3000000, 4000000);
// fiveMil = listings.slice(4000000, 5000000);
// sixMil = listings.slice(5000000, 6000000);
// sevenMil = listings.slice(6000000, 7000000);
// eightMil = listings.slice(7000000, 8000000);
// nineMil = listings.slice(8000000, 9000000);
// tenMil = listings.slice(9000000, 10000000);

// console.log(listings);
// let testDate = new Date();
// console.log(testDate);

// let parsedListings = JSON.stringify(listings);
// fs.appendFile('listingsTenMil.json', parsedListings, 'utf8', function(err) {
//     if(err) {
//         return console.log(err);
//     }
//     console.log("The file was saved!");
// }); 

// let getData = () => {
//     return new Promise((resolve,reject) => {
//         fs.readFile('./listingsTenMil.json','utf8', (err,data) => {
//             if (err){throw err};
//             resolve(data);
//         })
//     })
// }


// getData().then((data) =>{
//     console.log(data);
// })  
// console.log('RANDOM TIME:', randomTime(12, 12, 0, 30, 0, 23, 0, 60, 0, 60));
module.exports = {
    // getData:getData,
    // parsedListings:parsedListings,
    orderID:orderID,
    dataGenerator:dataGenerator
}