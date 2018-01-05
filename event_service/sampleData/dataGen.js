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
        //Do one or the other not both
        extCount--;
        intCount++;
        //
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


module.exports = {
    orderID:orderID,
    dataGenerator:dataGenerator
}