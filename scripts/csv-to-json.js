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
 * node csv-to-json.js
 * 
 * Create a json file for each review associated with the specified products.
 * Files will be stored in the `data/food_reviews` directory.
 * These files can then be uploaded into Watson Discovery for processing.
 */

const csvFilePath='../data/Reviews-full.csv';
const csv=require('csvtojson');
const fs=require('fs');

// list of products we want reviews for
const products = [
  'B002QWP89S',
  'B0013NUGDE',
  'B0026RQTGE',
  'B001EO5Q64',
  'B000VK8AVK',
  'B002QWHJOU',
  'B002QWP8H0',
  'B003B3OOPA',
  'B006HYLW32',
  'B007JFMH8M'
];

let padding = '000';  // to Line up the numbers and slice(-4).
let files = 0;
let myMap= new Map();

function logMapElements(value, key, map) {
  console.log(`map[${key}] = ${value}`);
}

csv({
  headers:['Id','ProductId','UserId','ProfileName','HelpfulnessNumerator','HelpfulnessDenominator','Score','Time','Summary','text'],
  noheader:false,
  colParser:{
    'HelpfulnessNumerator':'number',
    'HelpfulnessDenominator':'number',
    'Score':'number',
    'date':function(item) {
      return new Date(Number(item) * 1000).toISOString().substring(0, 10);
    }
  },
  checkType:false
})
  .fromFile(csvFilePath)
  .on('data',(jsonStr)=>{
    let json = JSON.parse(jsonStr);
    let key = json['ProductId'];

    if (products.includes(key)) {
      // console.log('Product we care about: ' + key);
      let numberOfReviews = 0;
      if (myMap.has(key)) {
        numberOfReviews = myMap.get(key);
      }
      if (numberOfReviews < 100) {
        numberOfReviews += 1;
        myMap.set(key, numberOfReviews);
        files += 1;
      
        fs.writeFile('../data/food_reviews/review_' + (padding + files).slice(-4) + '.json', jsonStr, (err) => {
          if (err) throw err;
          // console.log('The file has been saved!');
        });
      }
    }
  })
  .on('done',(error)=>{
    if (error) throw error;
    // console.log('end');
    console.log('totalReviews: ' + files);
    myMap.forEach(logMapElements);
  });
