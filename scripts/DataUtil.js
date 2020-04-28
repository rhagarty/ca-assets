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
  getStoreData: () => {
    let data = [];
    storeJson.forEach((store) => {
      data.push(store);
    });
    return data;
  },

  getProductData: () => {
    let data = [];
    productJson.forEach((product) => {
      data.push(product);
    });
    return data;
  },

  getWarehouseData: () => {
    let data = [];
    warehouseJson.forEach(function (warehouse) {
      productJson.forEach(function (product) {
        let items = randomIntFromInterval(500, 800);
        let amt = randomIntFromInterval(50, 100);
        data.push({
          warehouseId: warehouse.warehouseId,
          productId: product.productId,
          numItems: items,
          resupplyAmount: amt,
          resupplyInterval: 'monthly',
        });
      });
    });
    return data;
  },

  getOrderData: () => {
    let data = [];
    let orderCnt = 1;
    storeJson.forEach(function (store) {
      productJson.forEach(function (product) {
        let size = randomIntFromInterval(50, 100);
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

  getSalesData: () => {
    let data = [];
    storeJson.forEach(function (store) {
      productJson.forEach(function (product) {
        for (let month = 7; month <= 12; month++) {
          let daysInMonth = getDaysInMonth(month);
          for (let day = 1; day <= daysInMonth; day++) {
            let amt = randomIntFromInterval(1, 25);
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

  extractEnrichedData: (enrichedDatas) => {
    let data = [];
    let currentDate = new Date();
    enrichedDatas.forEach(function (enrichedData) {
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
      data.push({
        productId: enrichedData.ProductId,
        time: reviewDate,
        rating: enrichedData.Score,
        score: score,
        label: enrichedData.enriched_text[0].sentiment.label,
        summary: summary,
      });
    });
    return data;
  }

};

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

function randomIntFromInterval(min, max) {
  return Math.floor(Math.random() * (max - min + 1) + min);
}

function getDaysInMonth(month) {
  if (month == 9 || month == 11) {
    return 30;
  } else {
    return 31;
  }
}
