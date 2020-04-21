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
require('dotenv').config({
  silent: true,
  path: '../.env',
});
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const Query = require('./QueryConstants');
const ibmdb = require('ibm_db');
let warehouses = ['W1000-01', 'W1000-02'];
let stores = ['S1000-01', 'S1000-02', 'S1000-03', 'S1000-04', 'S1000-05'];
let products = [
  'P1000-01',
  'P1000-02',
  'P1000-03',
  'P1000-04',
  'P1000-05',
  'P1000-06',
  'P1000-07',
  'P1000-08',
  'P1000-09',
  'P1000-10',
];

const writeToCSV = process.env.WRITE_TO_CSV_FILE;

buildStoreFile()
.then((done) => {
  return buildProductFile()
}).then((done) => { 
  return buildWarehouseFile()
})
.then((done) => { 
  return buildOrderFile()
}).then((done) => { 
  return buildSalesfile();
}).catch((err)=> {
  console.log(err);
  return;
})


// write out each review as a row into a csv file
async function buildStoreFile() {
  console.log('Build Store File');

  const csvWriter = createCsvWriter({
    path: '../data/out-stores.csv',
    header: [
      { id: 'storeid', title: 'StoreId' },
      { id: 'address', title: 'Address' },
      { id: 'city', title: 'City' },
      { id: 'state', title: 'State' },
    ],
  });

  let data = [];

  data.push({
    product: stores[0],
    address: '123 Main St',
    city: 'Berkeley',
    state: 'CA',
  });
  data.push({
    storeid: stores[1],
    address: '222 Elm St',
    city: 'Walnut Creek',
    state: 'CA',
  });
  data.push({
    storeid: stores[2],
    address: '555 Maple St',
    city: 'Richmond',
    state: 'CA',
  });
  data.push({
    storeid: stores[3],
    address: '234 East Ave',
    city: 'Concord',
    state: 'CA',
  });
  data.push({
    storeid: stores[4],
    address: '100 Market St',
    city: 'San Francisco',
    state: 'CA',
  });

  if (writeToCSV === 'true') {
    csvWriter
      .writeRecords(data)
      .then(() => console.log('The CSV file was written successfully'));
  }

  await updateDB(Query.STORE_CREATE_TABLE, Query.STORE_INSERT_TO_TABLE, data);
  console.log("Store table created..")
  return true;
}

// write out each review as a row into a csv file
async function buildProductFile() {
  console.log('Build Product File');

  const csvWriter = createCsvWriter({
    path: '../data/out-products.csv',
    header: [
      { id: 'productid', title: 'ProductId' },
      { id: 'name', title: 'Name' },
      { id: 'price', title: 'Price' },
    ],
  });

  let data = [];

  data.push({
    productid: products[0],
    name: 'product A',
    price: 3.99,
  });
  data.push({
    productid: products[1],
    name: 'product B',
    price: 3.99,
  });
  data.push({
    productid: products[2],
    name: 'product C',
    price: 3.99,
  });
  data.push({
    productid: products[3],
    name: 'product D',
    price: 3.99,
  });
  data.push({
    productid: products[4],
    name: 'product E',
    price: 3.99,
  });
  data.push({
    productid: products[5],
    name: 'product F',
    price: 3.99,
  });
  data.push({
    productid: products[6],
    name: 'product G',
    price: 3.99,
  });
  data.push({
    productid: products[7],
    name: 'product H',
    price: 3.99,
  });
  data.push({
    productid: products[8],
    name: 'product I',
    price: 3.99,
  });
  data.push({
    productid: products[9],
    name: 'product J',
    price: 3.99,
  });

  if (writeToCSV === 'true') {
    csvWriter
      .writeRecords(data)
      .then(() => console.log('The CSV file was written successfully'));
  }

  await updateDB(
    Query.PRODUCT_CREATE_TABLE,
    Query.PRODUCT_INSERT_TO_TABLE,
    data
  );

  console.log("Product table created..")
  return true;
}

// write out each review as a row into a csv file
async function buildWarehouseFile() {
  console.log('Build Warehouse File');

  const csvWriter = createCsvWriter({
    path: '../data/out-warehouses.csv',
    header: [
      { id: 'warehouseid', title: 'WarehouseId' },
      { id: 'productid', title: 'ProductId' },
      { id: 'numitems', title: 'Number of Items' },
      { id: 'resupplyamount', title: 'Resupply Amount' },
      { id: 'resupplyinterval', title: 'Resupply Time Interval' },
    ],
  });

  let data = [];

  warehouses.forEach(function (warehouse) {
    products.forEach(function (product) {
      let items = randomIntFromInterval(500, 800);
      let amt = randomIntFromInterval(50, 100);
      data.push({
        warehouseid: warehouse,
        productid: product,
        numitems: items,
        resupplyamount: amt,
        resupplyinterval: 'monthly',
      });
    });
  });
  if (writeToCSV === 'true') {
    csvWriter
      .writeRecords(data)
      .then(() => console.log('The CSV file was written successfully'));
  }

  await updateDB(
    Query.WAREHOUSE_CREATE_TABLE,
    Query.WAREHOUSE_INSERT_TO_TABLE,
    data
  );
  console.log("Warehouse table created..")
  return true;
}

// write out each review as a row into a csv file
async function buildOrderFile() {
  console.log('Build Order File');

  const csvWriter = createCsvWriter({
    path: '../data/out-orders.csv',
    header: [
      { id: 'orderid', title: 'OrderId' },
      { id: 'date', title: 'Date' },
      { id: 'storeid', title: 'StoreId' },
      { id: 'productid', title: 'ProductId' },
      { id: 'itemprice', title: 'Item Price' },
      { id: 'ordersize', title: 'Number of Items' },
    ],
  });

  let data = [];
  let orderCnt = 1;

  stores.forEach(function (store) {
    products.forEach(function (product) {
      let size = randomIntFromInterval(50, 100);
      data.push({
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

  if (writeToCSV === 'true') {
    csvWriter
      .writeRecords(data)
      .then(() => console.log('The CSV file was written successfully'));
  }

  await updateDB(
    Query.STORE_ORDER_CREATE_TABLE,
    Query.STORE_ORDER_INSERT_TO_TABLE,
    data
  );
  console.log("Order table created..");
  return true;
}

// write out each review as a row into a csv file
async function buildSalesfile() {
  console.log('Build Sales File');

  const csvWriter = createCsvWriter({
    path: '../data/out-sales.csv',
    header: [
      { id: 'storeid', title: 'StoreId' },
      { id: 'productid', title: 'ProductId' },
      { id: 'date', title: 'Date' },
      { id: 'amount', title: 'Quantity' },
    ],
  });

  let data = [];

  stores.forEach(function (store) {
    products.forEach(function (product) {
      for (let month = 11; month <= 12; month++) {
        for (let day = 1; day < 31; day++) {
          let amt = randomIntFromInterval(1, 25);
          let date = month.toString() + '/' + day.toString() + '/2019';
          data.push({
            storeid: store,
            productid: product,
            date: date,
            amount: amt,
          });
        }
      }
    });
  });

  if (writeToCSV === 'true') {
    csvWriter
      .writeRecords(data)
      .then(() => console.log('The CSV file was written successfully'));
  }

  await updateDB(Query.SALES_CREATE_TABLE, Query.SALES_INSERT_TO_TABLE, data);
  console.log("Sales table created..")
  return true;
}

// write out each review into a Db2 table
async function updateDB(createQuery, insertQuery, data) {
  const connStr = process.env.DB2WH_DSN;
  return new Promise((resolve, reject) => {
    ibmdb.open(connStr, function (err, conn) {
      if (err) {
        console.log('DB Connection Error: ' + err);
        reject(err);
      }
      // note: table name can NOT contain a dash
      conn.querySync(createQuery);
      conn.prepare(insertQuery, function (err, stmt) {
        if (err) {
          console.log(err);
          conn.closeSync();
          reject(err);
        }

        let i = 0;
        let keys = [];
        data.forEach(function (row) {
          Object.keys(row).forEach(function (key) {
            keys.push(row[key]);
          });         
          stmt.executeNonQuerySync(keys);
          i += 1;
          keys = [];
        });

        stmt.closeSync();
        console.log('Closing connection. Records written: ' + i);
        conn.closeSync();
        resolve();
      });
    });
  });
}

function randomDate() {
  function randomValueBetween(min, max) {
    return Math.random() * (max - min) + min;
  }
  let date1 = '11-01-2019';
  let date2 = '12-31-2019';
  date1 = new Date(date1).getTime();
  date2 = new Date(date2).getTime();
  if (date1 > date2) {
    return new Date(randomValueBetween(date2, date1)).toLocaleDateString();
  } else {
    return new Date(randomValueBetween(date1, date2)).toLocaleDateString();
  }
}

function randomIntFromInterval(min, max) {
  return Math.floor(Math.random() * (max - min + 1) + min);
}
