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

const storeJson = require('../data/store.json');
const productJson = require('../data/product.json');
const warehouseJson = require('../data/warehouse.json');

module.exports = {

  // STORE DATA
  getStoreData: () => {
    let data = [];
    storeJson.forEach((store) => {
      data.push(store);
    });
    return data;
  },

  // PRODUCT DATA
  getProductData: () => {
    let data = [];
    productJson.forEach((product) => {
      let amt = randomIntFromInterval(50, 100);
      data.push({
        productId: product.productId,
        name: product.name,
        price: product.price,
        restockAmt: amt,
        restockInt: 'monthly',
      });
    });
    return data;
  },

  // WAREHOUSE DATA
  getWarehouseData: () => {
    let data = [];
    warehouseJson.forEach(function (warehouse) {
      productJson.forEach(function (product) {
        for (let month = 7; month <= 12; month++) {
          let items = randomIntFromInterval(500, 800);
          let date = month.toString() + '/1/2019';
          data.push({
            warehouseId: warehouse.warehouseId,
            productId: product.productId,
            date: date,
            numItems: items
          });
        }
      });
    });
    return data;
  },

  // ORDER DATA
  getOrderData: () => {
    let data = [];
    let orderCnt = 1;
    storeJson.forEach(function (store) {
      productJson.forEach(function (product) {
        let size = randomIntFromInterval(20, 50);
        data.push({
          orderId: 'ORD-' + orderCnt,
          date: randomDate(),
          storeId: store.storeId,
          productId: product.productId,
          itemPrice: 3.99,
          orderSize: size,
        });
        orderCnt += 1;
      });
    });

    return data;
  },

  // SALES DATA
  getSalesData: () => {
    let data = [];
    storeJson.forEach(function (store) {
      productJson.forEach(function (product) {
        for (let month = 7; month <= 12; month++) {
          let daysInMonth = getDaysInMonth(month);
          for (let day = 1; day <= daysInMonth; day++) {
            let amt;
            if (product.name === 'Cappuccino') {
              // most popular
              amt = randomIntFromInterval(15, 40);
            } else if (product.name === 'Columbian') {
              // least popular
              amt = randomIntFromInterval(1, 10);
            } else {
              amt = randomIntFromInterval(10, 20);
            }
            let date = month.toString() + '/' + day.toString() + '/2019';
            data.push({
              storeId: store.storeId,
              productId: product.productId,
              date: date,
              amount: amt,
            });
          }
        }
      });
    });
    return data;
  },

  // KEYWORD DATA
  extractKeywordData: (enrichedDatas) => {
    let data = [];
    let keywords = [];  // array of objects, one for each product

    enrichedDatas.forEach(function (enrichedData) {
      // get object for this product
      let found = false;
      let keywordObj;
      keywords.forEach(function (keyword) {
        if (keyword.productId === enrichedData.ProductId) {
          keywordObj = keyword;
          found = true;
        }
      });
      if (!found) {
        keywordObj = {
          productId: enrichedData.ProductId,
          keywords: []
        }
      }
      
      // iterate through all keywords for a single review
      enrichedData.enriched_text[0].keywords.forEach(function(entry) {
        // determine if we have started collecting keywords for this product
        keywords.forEach(function (keyword) {
          // find out if this keyword has been used before
          found = false;
          for (var i=0; i<keywordObj.keywords.length; i++) {
            if (keywordObj.keywords[i].keyword === keyword) {
              keywordObj.keywords[i].count += 1;
              found = true;
            }
          }
          if (!found) {
            keywordObj.keywords.push({
              keyword: keyword,
              count: 1
            });
          }
        });
      });
    });
    console.log('keywordObj: ' + JSON.stringify(keywordObj, null, 2));
    return data;
  },

  // Get review data from Discovery
  extractEnrichedData: (enrichedDatas) => {
    let data = [];
    let currentDate = new Date();

    enrichedDatas.forEach(function (enrichedData) {
      // console.log(JSON.stringify(enrichedData, null, 2));
      
      //
      // first get review data
      //

      // convert timestamp to string
      let dd = new Date(enrichedData.Time * 1000);
      let month = dd.getMonth() + 1;
      let day = dd.getDate();
      // all reviews are 2012 or older, so making them all CurrentYear -1
      let year = currentDate.getFullYear() - 1;
  
      // hack for leap year months.
      if (month === 2 && day === 29) {
        day = day - 1;
      }
  
      let monthStr = '' + month;
      let dayStr = '' + day;
      if (month < 10) monthStr = '0' + month;
      if (day < 10) dayStr = '0' + day;
  
      let reviewDate = [year, monthStr, dayStr].join('-');
      let summary = enrichedData.Summary.substring(0, 120);
      let score = enrichedData.enriched_text[0].sentiment.score.toFixed(6);
      // console.log(JSON.stringify(enrichedData.enriched_text[0], null, 2));
      // console.log(JSON.stringify(enrichedData, null, 2));

      let idx = getProductObject(data, enrichedData.ProductId);
      if (idx < 0) {
        // not found, so create
        obj = {
          productId: enrichedData.ProductId,
          reviews: [{
            time: reviewDate,
            rating: enrichedData.Score,
            score: score,
            label: enrichedData.enriched_text[0].sentiment.label,
            summary: summary,
          }],
          keywords: []
        }
        data.push(obj);
      } else {
        data[idx].reviews.push({
          time: reviewDate,
          rating: enrichedData.Score,
          score: score,
          label: enrichedData.enriched_text[0].sentiment.label,
          summary: summary,
        });
      }

      //
      // now get keyword data
      //

      // iterate through all keywords for the review
      idx = getProductObject(data, enrichedData.ProductId);
      enrichedData.enriched_text[0].keywords.forEach(function(keyword) {
        // find out if this keyword has been used before
        found = false;
        for (var i=0; i<data[idx].keywords.length; i++) {
          if (data[idx].keywords[i].text === keyword.text) {
            data[idx].keywords[i].count += 1;
            found = true;
          }
        }
        if (!found) {
          data[idx].keywords.push({
            text: keyword.text,
            count: 1
          });
        }
      });
    });

    // keep only the most popular keywords
    let popularKeywords = [];
    data.forEach(function(entry) {
      entry.keywords.forEach(function(keyword) {
        if (keyword.count > 5) {
          popularKeywords.push({
            text: keyword.text,
            count: keyword.count
          });
          }
      });

      entry.keywords = popularKeywords;
    });

    // console.log(JSON.stringify(data[0].keywords, null, 2));
    return data;
  }
};

// generate a random date
function randomDate() {
  function randomValueBetween(min, max) {
    return Math.random() * (max - min) + min;
  }
  let date1 = '07-01-2019';
  let date2 = '12-31-2019';
  date1 = new Date(date1).getTime();
  date2 = new Date(date2).getTime();
  if (date1 > date2) {
    return new Date(randomValueBetween(date2, date1)).toLocaleDateString();
  } else {
    return new Date(randomValueBetween(date1, date2)).toLocaleDateString();
  }
}

// generate a random number between min/max
function randomIntFromInterval(min, max) {
  return Math.floor(Math.random() * (max - min + 1) + min);
}

// return days in specific month
function getDaysInMonth(month) {
  if (month == 9 || month == 11) {
    return 30;
  } else {
    return 31;
  }
}

function getProductObject(data, productId) {
  // get object for this product
  for (var idx = 0; idx < data.length; idx++) {
    if (data[idx].productId === productId) {
      return idx;
    }
  }

  return -1;
}
