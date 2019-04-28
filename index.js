"use strict";

const fs = require("fs");
const csvParseSync = require("csv-parse/lib/sync");
const AWS = require("aws-sdk");
AWS.config.update({region: "ap-northeast-1"});
const docClient = new AWS.DynamoDB.DocumentClient();
const unmarshalItem = require("dynamodb-marshaler").unmarshalItem;

//Config 
const targetTableName = "bid_groups-dev";
const csv_filename = "./data/bid_groups-dev20181112.csv";
const sleep_milsecond = 2000;
//

async function main(){

    try {


        if(!targetTableName.match(/-/)) throw "Check targetTabeleName! Production stage?";

        let data = fs.readFileSync(csv_filename);
        let dataCSV = csvParseSync(data);

        console.log("csv data was imported.")

        let maxRow = dataCSV.length -1;

        let arrayKeys = createItemKey(dataCSV[0]);

        console.log("Start to import...")

        for(let i =1; i< dataCSV.length; i++){

            let targetItem = createItemData(arrayKeys, dataCSV[i]);

            console.log("Item:", targetItem);

            //Todo batchWrite
            let params = {
                TableName: targetTableName,
                Item: targetItem
            };

            await docClient.put(params).promise();

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

    let arrayKeys = [];
    
    let arrayStr = data.toString().split(",");

    for(let i=0; i<arrayStr.length; i++){

        let keyObj = {};

        let result = arrayStr[i].match(/ \(.*\)/);

        let type = result[0].replace(/ \(|\)/g, "");

        keyObj.type = type;

        keyObj.name = arrayStr[i].toString().replace(/ \(.*\)/g, "");


        //console.log("type:",type);
        //console.log("keyObj:",keyObj);

        arrayKeys.push(keyObj);

    }

    return arrayKeys;

}

function createItemData(keys, data){

    let obj = {};

    for(let i=0; i< keys.length; i++){

        data[i] = data[i].toString().replace(/\"\"/g, "\"");
        
        console.log(i + ". key:" + keys[i].name + " type:" + keys[i].type);
        console.log("data:",data[i]);

        //空文字対策
        //if(data[i] === "") data[i] = false;
        if(data[i] === "") {
            //console.log("skip!");
            continue;
        }

        //keys[i].typeにより型変換
        switch (keys[i].type) {
            case "S":
                data[i] = data[i].toString();
                if(data[i] == "null" || data[i] == "false") data[i] = false;
                break;
            case "N":
                data[i] = Number(data[i]);
                if(isNaN(data[i])) data[i] = false;
                break;            
            case "L":
                data[i] = JSON.parse(data[i]);
                data[i] = transformMap("L", data[i])
                break;
            case "M":
                data[i] = JSON.parse(data[i]);
                data[i] = transformMap("M", data[i])
                break;
            case "BOOL":
                data[i] = toBoolean(data[i]);
                break;
            default:
              console.log("Sorry, we are out of " + keys[i].type + ".");
          }

        //console.log("data:",data[i]);
        
        obj[keys[i].name] = data[i];

    }

    return obj;

}

function transformMap(type, data){

    //console.log("data in transformMap:",data);

    let Item = null;

    switch (type) {         
        case "L":
            Item = { tempKey:  {L:data}}
            break;
        case "M":
            Item = { tempKey:  {M:data}}
            break; 
        default:
          console.log("Sorry, we are out of " + type + ".");
      }

    //console.log("Item:", Item);

    Item = unmarshalItem(Item)

    return Item.tempKey; 

}

function toBoolean(data){

    return data.toString().toLowerCase() === "true";

}