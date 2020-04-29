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
 * node generate-disco-input-files.js
 * 
 * Create a json file for each review associated with the specified products.
 * Files will be stored in the `data/food_reviews` directory.
 * These files can then be uploaded into Watson Discovery for processing.
 */

const csvFilePath='../data/Reviews-full.csv';
const csv=require('csvtojson');
const fs=require('fs');

// list of products we want reviews for, and the NEW product ID we will use to make visuals easier to read
const products = [
  { id: 'B001VJ0B0I', newId: 'P1000-01' },
  { id: 'B005K4Q37A', newId: 'P1000-02' },
  { id: 'B000KV61FC', newId: 'P1000-03' },
  { id: 'B001EO5Q64', newId: 'P1000-04' },
  { id: 'B003B3OOPA', newId: 'P1000-05' }
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

    let matchingObj = products.find(o => o.id === key);
    if (matchingObj) {
      // console.log('Product we care about: ' + key);
      let numberOfReviews = 0;
      if (myMap.has(key)) {
        numberOfReviews = myMap.get(key);
      }
      if (numberOfReviews < 200 && json['HelpfulnessNumerator'] > 0) {
          numberOfReviews += 1;
        myMap.set(key, numberOfReviews);
        files += 1;
      
        // replace product ID with user-friendly ID
        json['ProductId'] = matchingObj.newId;
        let str = JSON.stringify(json);
        fs.writeFile('../data/food_reviews/review_' + (padding + files).slice(-4) + '.json', str, (err) => {
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
