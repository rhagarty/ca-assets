/**
 * Copyright 2020 IBM Corp. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License'); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * To Run:
 * cd scripts
 * node store-disco-enrichments.js
 * 
 * Queries Watson Discovery to retrieve all reviews which include sentiment data.
 * That data is then written out to build a new csv file - 'data/Reviews-with-sentiment.csv'.
 * Each record is also written to a Db2 instance. Instructions for how to do this:
 * https://developer.ibm.com/mainframe/2019/08/07/accessing-ibm-db2-on-node-js/
 * https://github.com/ibmdb/node-ibm_db/blob/master/APIDocumentation.md
 */

const createCsvWriter = require('csv-writer').createObjectCsvWriter;

let warehouses = ['W1000-01', 'W1000-02'];
let stores = ['S1000-01', 'S1000-02', 'S1000-03', 'S1000-04', 'S1000-05'];
let products = ['P1000-01', 'P1000-02','P1000-03','P1000-04','P1000-05','P1000-06','P1000-07','P1000-08','P1000-09','P1000-10'];

buildStoreFile();
buildOrderFile();
buildWarehouseFile();
buildSalesfile();
//updateDb();

// write out each review as a row into a csv file
function buildStoreFile() {
  console.log('Build Store File');

  const csvWriter = createCsvWriter({
    path: '../data/out-stores.csv',
    header: [
      {id: 'storeid', title: 'StoreId' },
      {id: 'address', title: 'Address' },
      {id: 'city', title: 'City' },
      {id: 'state', title: 'State' },
    ]
  });
  
  let data = [];

  data.push({ storeid: stores[0], address: '123 Main St', city: 'Berkeley', state: 'CA'});
  data.push({ storeid: stores[1], address: '222 Elm St', city: 'Walnut Creek', state: 'CA'});
  data.push({ storeid: stores[2], address: '555 Maple St', city: 'Richmond', state: 'CA'});
  data.push({ storeid: stores[3], address: '234 East Ave', city: 'Concord', state: 'CA'});
  data.push({ storeid: stores[4], address: '100 Market St', city: 'San Francisco', state: 'CA'});  

  csvWriter
    .writeRecords(data)
    .then(()=> console.log('The CSV file was written successfully'));
}

// write out each review as a row into a csv file
function buildWarehouseFile() {
  console.log('Build Warehouse File');

  const csvWriter = createCsvWriter({
    path: '../data/out-warehouses.csv',
    header: [
      {id: 'warehouseid', title: 'WarehouseId' },
      {id: 'productid', title: 'ProductId' },
      {id: 'numitems', title: 'Number of Items' },
      {id: 'resupplyamount', title: 'Resupply Amount' },
      {id: 'resupplyinterval', title: 'Resupply Time Interval' },
    ]
  });
  
  let data = [];

  warehouses.forEach(function(warehouse) {
    products.forEach(function(product) {
      let items = randomIntFromInterval(500, 800);
      let amt = randomIntFromInterval(50, 100);
      data.push(
        { 
          warehouseid: warehouse,
          productid: product,
          numitems: items,
          resupplyamount: amt,
          resupplyinterval: 'monthly',
        });
    });
  });  
  
  csvWriter
    .writeRecords(data)
    .then(()=> console.log('The CSV file was written successfully'));
}

// write out each review as a row into a csv file
function buildOrderFile() {
  console.log('Build Order File');

  const csvWriter = createCsvWriter({
    path: '../data/out-orders.csv',
    header: [
      {id: 'orderid', title: 'OrderId' },
      {id: 'date', title: 'Date' },
      {id: 'storeid', title: 'StoreId' },
      {id: 'productid', title: 'ProductId' },
      {id: 'itemprice', title: 'Item Price' },
      {id: 'ordersize', title: 'Number of Items' },
    ]
  });
  
  let data = [];
  let orderCnt = 1;

  stores.forEach(function(store) {
    products.forEach(function(product) {
      let size = randomIntFromInterval(50, 100);
      data.push(
        { 
          orderid: 'ORD-' + orderCnt,
          date: randomDate(),
          storeid: store,
          productid: product,
          itemprice: 3.99,
          ordersize: size,
        });
      orderCnt += 1;
    });
  });  
  
  csvWriter
    .writeRecords(data)
    .then(()=> console.log('The CSV file was written successfully'));
}

// write out each review as a row into a csv file
function buildSalesfile() {
  console.log('Build Sales File');

  const csvWriter = createCsvWriter({
    path: '../data/out-sales.csv',
    header: [
      {id: 'storeid', title: 'StoreId' },
      {id: 'productid', title: 'ProductId' },
      {id: 'date', title: 'Date' },
      {id: 'amount', title: 'Quantity' },
    ]
  });
  
  let data = [];

  stores.forEach(function(store) {
    products.forEach(function(product) {
      for (let month = 11; month <= 12; month++) {
        for (let day = 1; day <= 31; day++) {
          let amt = randomIntFromInterval(1, 25);
          let date = month.toString() + '/' + day.toString() + '/2019';
          data.push(
            { 
              storeid: store,
              productid: product,
              date: date,
              amount: amt,
            });
        }
      }
    });
  });  
  
  csvWriter
    .writeRecords(data)
    .then(()=> console.log('The CSV file was written successfully'));
}

// write out each review into a Db2 table
function updateDB(results) {
  // const connStr = 'DATABASE=BLUDB;HOSTNAME=dashdb-txn-sbox-yp-dal09-08.services.dal.bluemix.net;PORT=50000;PROTOCOL=TCPIP;UID=wdp34011;PWD=6k2tx7mk+499cgsq;';
  const connStr = process.env.DB2_DSN;

  ibmdb.open(connStr, function (err, conn) { 
    if (err) {
      console.log('DB Connection Error: ' + err);
      return;
    }
    
    console.log('opened connection');
    // note: table name can NOT contain a dash, and ensure summary text does not exceed max length as defined here
    conn.querySync("create table FOOD_REVIEWS (ProductId varchar(10), Time int, Rating smallint, Sentiment_Score decimal(12,6), Sentiment_Label varchar(8), Summary varchar(120))");
    conn.prepare("insert into FOOD_REVIEWS (ProductId, Time, Rating, Sentiment_Score, Sentiment_Label, Summary) Values (?, ?, ?, ?, ?, ?)", function (err, stmt) {
      if (err) {
        console.log(err);
        return conn.closeSync();
      }

      let i = 0;
      results.forEach(function(result) {
        let summary = result.Summary.substring(0, 120);

        // convert timestamp to mm/dd/yyyy
        // console.log(result.ProductId + ',' + result.Time + ',' + result.Score + ',' + result.enriched_text.sentiment.document.score + ',' + 
        //   result.enriched_text.sentiment.document.label + ',' + summary + ' [' + summary.length + ']');
        stmt.executeNonQuerySync([ result.ProductId, result.Time, result.Score, result.enriched_text.sentiment.document.score, result.enriched_text.sentiment.document.label, summary ]);
        i += 1;
      });

      stmt.closeSync();
      console.log('Closing connection. Records written: ' + i);
      conn.closeSync();
    });
  });
}

function randomDate(){
  function randomValueBetween(min, max) {
    return Math.random() * (max - min) + min;
  }
  let date1 = '11-01-2019';
  let date2 = '12-31-2019';
  date1 = new Date(date1).getTime()
  date2 = new Date(date2).getTime()
  if( date1>date2){
      return new Date(randomValueBetween(date2,date1)).toLocaleDateString()   
  } else{
      return new Date(randomValueBetween(date1, date2)).toLocaleDateString()  
  }
}

function randomIntFromInterval(min, max) {
  return Math.floor(Math.random() * (max - min + 1) + min);
}
