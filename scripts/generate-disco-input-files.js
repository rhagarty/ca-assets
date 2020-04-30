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
  { id: 'B005ZBZLT4', newId: 'P1000-01' },  // Fog Chaser
  { id: 'B005K4Q37A', newId: 'P1000-02' },  // Cappuccino
  { id: 'B006Q820X0', newId: 'P1000-03' },  // Dark Roast
  { id: 'B00438XVGU', newId: 'P1000-04' },  // Columbian
  { id: 'B007I7Z3Z0', newId: 'P1000-05' }   // Black Tea
];

// B000UBD88A[158] usefullness: 0.8811881188118812   // Senseo Coffee Pods, Dark Roast
// B001D0IZBM[160] usefullness: 0.8732106339468303   // Emeril's Big Easy Bold Coffee
// B003D4F1QS[167] usefullness: 0.8620689655172413   // Stash Tea Decaf Premium Green Tea
// B007I7Z3Z0[161] usefullness: 0.6775193798449612   // Lipton To Go Stix Iced Black Tea Mix
// B00438XVGU[168] usefullness: 0.8112449799196787   // Starbucks VIA Ready Brew Coffee, Colombia
// B006Q820X0[189] usefullness: 0.5523877405559515   // Brooklyn Bean Roastery Coffee
// B005K4Q37A[241] usefullness: 0.791459781529295    // Grove Square Caramel Cappuccino
// B005ZBZLT4[244] usefullness: 0.7641144624903325   // San Francisco Bay OneCup, Fog Chaser Coffee

// B002QWP89S[162] usefullness: 0.7154566744730679   // Greenies Dental Dog Treats
// B000FI4O90[167] usefullness: 0.9106130860381246   // AeroGarden 7 with Gourmet Herb Seed Kit
// B0019QT66I[174] usefullness: 0.8171109733415995   // Natural Vitality Calcium Drink
// B000UUWECC[177] usefullness: 0.8215038650737878   // ONE Coconut Water
// B000NMJWZO[189] usefullness: 0.8444180522565321   // Pamela's Products Glutten Free Baking and Pancake Mix
// B003TNANSO[190] usefullness: 0.87001287001287     // Kind Plus, Peanut Butter Dark Chocolate
// B001D09KAM[190] usefullness: 0.8696774193548387   // Kind Plus, Mango Macadamia Bars
// B003QNJYXM[193] usefullness: 0.7087301587301588   // 5-hour Energy, Extra Strength Berry
// B008ZRKZSM[194] usefullness: 0.8621987951807228   // Pb2 Powdered Peanut Butter
// B0013NUGDE[195] usefullness: 0.7589424572317263   // Popchips, Sea Salt & Vinegar
// B004SRH2B6[198] usefullness: 0.7479055597867479   // ZICO Pure Premium Coconut Water
// B0018KR8V0[210] usefullness: 0.7197732997481109   // Larabar Glutten Free Bar, Key Lime Pie
// B001AS1A4Q[210] usefullness: 0.7136659436008677   // 5-hour Energy, Berry
// B000GAT6NG[211] usefullness: 0.861442362294151    // Nutiva Organic Cold-Press Virgin Coconut Oil
// B001VJ0B0I[215] usefullness: 0.7291242362525459   // Purina Beneful Originals Adult Dry Dog Food
// B000VK08OC[222] usefullness: 0.8879135719108711   // Wedderspoon Raw Premium Manuka Honey
// B008J1HO4C[227] usefullness: 0.8479318734793188   // McCann's Steel Cut Oatmeal
// B004CLCEDE[235] usefullness: 0.9008547008547009   // Miracle Noodle Shirataki Angel Hair Pasta
// B000KV61FC[316] usefullness: 0.9059982094897046   // PetSafe Busy Buddy Tug-A-Jug Meal-Dispensing Dog Toy
// B001EO5Q64[335] usefullness: 0.8498472282845919   // Nutiva Organic Cold-Pressed Virgin Coconut Oil
// B003B3OOPA[428] usefullness: 0.8811733014067644   // Nature's Way Coconut Oil

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
      if (numberOfReviews < 240 && json['HelpfulnessNumerator'] > 0) {
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
