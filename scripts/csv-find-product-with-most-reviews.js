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
 * copy Reviews-full.csv to /data directory
 * cd scripts
 * node csv-find-projuct-with-most-reviews.js
 * 
 * Utility to pull out all products that have at least `minLevelOfReviews`.
 */

const csvFilePath='../data/Reviews-full.csv';
const csv=require('csvtojson');

let totalReviews = 0;
let minLevelOfReviews = 550;
let myMap= new Map();

function logMapElements(value, key, map) {
  if (value > minLevelOfReviews) {
    console.log(`map[${key}] = ${value}`);
  }
}

csv({
  headers:['Id','ProductId','UserId','ProfileName','HelpfulnessNumerator','HelpfulnessDenominator','Score','Time','Summary','Text'],
  noheader:false,
  colParser:{
    'HelpfulnessNumerator':'number',
    'HelpfulnessDenominator':'number',
    'Score':'number',
    'Time':function(item) {
      return new Date(Number(item) * 1000).toISOString().substring(0, 10);
    }
  },
  checkType:false
})
  .fromFile(csvFilePath)
  .on('data',(jsonStr)=>{
    let json = JSON.parse(jsonStr);
    let key = json['ProductId'];
    //console.log(key);

    if (myMap.has(key)) {
      // console.log('key exists');
      let val = myMap.get(key) + 1;
      myMap.set(key, val);
    } else {
      // console.log('key does not exist');
      myMap.set(key, 1);
    }
    totalReviews = totalReviews + 1;
  })
  .on('done',(error)=>{
    if (error) throw error;
    // console.log('end');
    console.log('totalReviews: ' + totalReviews);
    myMap.forEach(logMapElements);
  });
