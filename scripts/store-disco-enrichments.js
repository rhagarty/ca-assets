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
  silent: true
});

const ibmdb = require('ibm_db');
const DiscoveryV1 = require('ibm-watson/discovery/v1');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const discovery = new DiscoveryV1({
  version: '2019-04-30'
});

const queryParams = {
  environmentId: process.env.DISCOVERY_ENVIRONMENT_ID,
  collectionId: process.env.DISCOVERY_COLLECTION_ID,
  count: 1000,
  passages: false
};

discovery.query(queryParams)
  .then(queryResponse => {
    // console.log(JSON.stringify(queryResponse, null, 2));
    return queryResponse.result;
  })
  .then(result => {
    console.log('+++ DISCO QUERY RESULTS +++');
    console.log('numMatches: ' + result.matching_results);
    buildcvsfile(result.results);
    return result;
  })
  // .then(result => {
  //   updateDB(result.results);
  // })
  .catch(err => {
    console.log('error:', err);
  });

// write out each review as a row into a csv file
function buildcvsfile(results) {
  console.log('Build CVS File');

  const csvWriter = createCsvWriter({
    path: '../data/out.csv',
    header: [
      {id: 'productid', title: 'ProductId' },
      {id: 'time', title: 'Time' },
      {id: 'rating', title: 'Rating' },
      {id: 'score', title: 'Sentiment Score' },
      {id: 'label', title: 'Sentiment Label' },
      {id: 'summary', title: 'Summary' },
    ]
  });
  
  let data = [];

  results.forEach(function(result) {
    // console.log('Result: ' + JSON.stringify(result, null, 2));
    data.push(
      { 
        productid: result.ProductId,
        time: result.Time,
        rating: result.Score,
        score: result.enriched_text.sentiment.document.score,
        label: result.enriched_text.sentiment.document.label,
        summary: result.Summary,
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
