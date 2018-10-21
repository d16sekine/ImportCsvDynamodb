"use strict";

const fs = require('fs');
const csvParseSync = require('csv-parse/lib/sync');
const AWS = require('aws-sdk');
AWS.config.update({region: 'ap-northeast-1'});
const docClient = new AWS.DynamoDB.DocumentClient();

//Config 
const targetTableName = "auction_groups-dev";
const csv_filename = "./auctions.csv";
const sleep_milsecond = 1000;
//

async function main(){

    try {
        let data = fs.readFileSync(csv_filename);
        let dataCSV = csvParseSync(data);

        console.log("csv data was imported.")

        let maxRow = dataCSV.length -1;

        let arrayKeys = createItemKey(dataCSV[0]);

        console.log("Start to import...")

        for(let i =1; i< dataCSV.length; i++){

            let targetItem = createItemData(arrayKeys, dataCSV[i]);

            //console.log("Item:", targetItem);

            //Todo batchWrite
            let params = {
                TableName: targetTableName,
                Item: targetItem
            };

            //await docClient.put(params).promise();

            console.log(i +" of " + maxRow + " row data was imported.")

            await new Promise(r => setTimeout(r, sleep_milsecond));
            
        }

        console.log("All data was imported!")

    }catch(err){
        console.log("err:", err);
    }

}

main();

function createItemKey(data){

    let strData = data.toString().replace(/ \(.\)/g, "");

    //console.log("data:", strData);

    let arrayStr = strData.split(",");

    return arrayStr;

}

function createItemData(keys, data){

    let obj = {};

    for(let i=0; i< keys.length; i++){

        //空文字対策
        if(data[i] === "") data[i] = false;

        obj[keys[i]] = data[i];

        if(keys[i] == "auction_gid") obj[keys[i]] = parseInt(data[i],10);

        if(data[i].match(/\[.*\]/g) !== null) {

            // data[i] = data[i].replace(/   /g,"");

            //let array = data[i].split(",")

            let json = JSON.parse(data[i]);

            //console.log("json:",json);

            console.log("json.length:",json.length);

            for(let i=0; i< json.length; i++){

                console.log("json[i]:", json[i]);

            }

            //console.log("Item:", data[i]);
            //console.log("Array:", array);


        }



    }

    return obj;

}