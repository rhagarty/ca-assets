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
let arr = [];

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

    // only use review if someone found it useful
    if (json['HelpfulnessNumerator'] > 0) {
      let idx = -1;
      let obj = arr.find((o, i) => {
        if (o.key === key) {
          // found it
          // console.log('found');
          idx = i;
          arr[i] = {
            key: key,
            number: o.number += 1,
            numerator: o.numerator += json['HelpfulnessNumerator'],
            denominator: o.denominator += json['HelpfulnessDenominator']
          };
          return true;
        }
      });
  
      if (idx < 0) {
        // console.log('not found');
        arr.push({
          key: key,
          number: 1,
          numerator: json['HelpfulnessNumerator'],
          denominator: json['HelpfulnessDenominator']
        });
      }
      totalReviews = totalReviews + 1;
    }
  })
  .on('done',(error)=>{
    if (error) throw error;
    // console.log('end');
    console.log('totalReviews: ' + totalReviews);

    // grab just the top ones
    let arr2 = [];
    arr.forEach(function (item, index) {
      if (item.number > 150) {
        arr2.push(item);
      }
    });

    // remove dups
    console.log('removing dups');
    let arr3 = [];
    arr2.forEach(function (item2, index2) {
      let dupFound = false;
      arr3.forEach(function (item3, index3) {
        if (item3.number === item2.number && item3.numerator === item2.numerator && item3.denominator === item2.denominator) {
          dupFound = true;
          return true;
        }
      });
      if (! dupFound) {
        arr3.push(item2);
      }
    });

    // sort
    arr3.sort(function(a, b) {
      return a.number - b.number;
    });
    console.log(JSON.stringify(arr3, null, 2));

    arr3.forEach(function (item, index) {
      console.log(item.key + '[' + item.number + '] usefullness: ' + item.numerator / item.denominator);
    });
  });
