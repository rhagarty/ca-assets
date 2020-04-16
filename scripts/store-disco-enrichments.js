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

const Query = require('./QueryConstants')

const ibmdb = require('ibm_db');
const DiscoveryV2 = require('ibm-watson/discovery/v2');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const discovery = new DiscoveryV2({
  version: '2019-04-30'
});

const queryParams = {
  projectId: process.env.DISCOVERY_PROJECT_ID,
  count: 1000
};

const writeToCSV = process.env.WRITE_TO_CSV_FILE;

discovery.query(queryParams)
  .then(queryResponse => {
    //console.log(JSON.stringify(queryResponse, null, 2));
    return queryResponse.result;
  })
  .then(result => {
    return extractData(result.results);
  })
  .then(result => {
    console.log('+++ DISCO QUERY RESULTS +++');
    console.log('numMatches: ' + result.matching_results);
    if(writeToCSV === 'true'){
      buildCSVfile(result);
    }
    return result;
  })
  .then(result => {
    updateDB(result);
  })
  .catch(err => {
    console.log('error:', err);
  });

  /**
   * Extracting relevant data for writing to CSV and/or DB
   * @param {*} results 
   */
function extractData(results) {
  console.log('Extracting relevant data from the result.')
  let data = [];
  let currentDate = new Date();
  results.forEach(function(result) {
    // convert timestamp to string
    let dd = new Date(result.Time * 1000);
    let month = dd.getMonth() + 1;
    let day = dd.getDate();
    // all reviews are 2012 or older, so making them all CurrentYear -1
    let year = currentDate.getFullYear() - 1;

    // hack for leap year months.
    if(month === 2 && day === 29){
      day = day - 1;
    }

    let monthStr = '' + month;
    let dayStr = '' + day;
    if (month < 10) 
      monthStr = '0' + month;
    if (day < 10)
      dayStr = '0' + day;

  
    let reviewDate = [year, monthStr, dayStr].join('-');
    data.push(
      { 
        productid: result.ProductId,
        time: reviewDate,
        rating: result.Score,
        score: result.enriched_text[0].sentiment.score,
        label: result.enriched_text[0].sentiment.label,
        summary: result.Summary,
      });
  });
  return data;
}  

// write out each review as a row into a csv file
function buildCSVfile(results) {
  console.log('Build CSV File');
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

  csvWriter
    .writeRecords(results)
    .then(()=> console.log('The CSV file was written successfully'));
}

// write out each review into a Db2 table
function updateDB(results) {
  const connStr = process.env.DB2WH_DSN;

  ibmdb.open(connStr, function (err, conn) { 
    if (err) {
      console.log('DB Connection Error: ' + err);
      return;
    }
    
    console.log('opened connection');
    // note: table name can NOT contain a dash, and ensure summary text does not exceed max length as defined here
    // if table already exists, then it will still run with a warning message
    // `Warning message Table or view <tablename> already exists.. SQLCODE=4136, SQLSTATE=, DRIVER=4.26.14`
    conn.querySync(Query.CREATE_TABLE);
    conn.prepare(Query.INSERT_TO_TABLE, function (err, stmt) {
      if (err) {
        console.log(err);
        return conn.closeSync();
      }

      let i = 0;
      results.forEach(function(result) {
        console.log(result)
        let summary = result.summary.substring(0, 120); 
        let score = result.score.toFixed(6);       

        stmt.executeNonQuerySync([ result.productid, result.time, result.rating, score, result.label, summary ]);
        i += 1;
      });

      stmt.closeSync();
      console.log('Closing connection. Records written: ' + i);
      conn.closeSync();
    });
  });
}
