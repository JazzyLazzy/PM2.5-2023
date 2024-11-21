const axios = require('axios');
const fs = require('fs');
const { Parser } = require('json2csv');

const baseUrl = "https://aqs.epa.gov/data/api/dailyData/byState";

//enter email and key
const email = "";
const key = "";

const param = "88101";
const edate = '20231231'//new Date(Date.now() - 1 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10).replace(/-/g, '');
const bdate = '20230101'//new Date(Date.now() - 1 * 2 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10).replace(/-/g, '');

// List of state codes (01 to 56, except for codes 03 and 07)
const stateCodes = Array.from({ length: 56 }, (_, i) => String(i + 2).padStart(2, '0')).filter(code => !['03', '07'].includes(code));

// Function to query each state's data
async function queryStateData(stateCode) {
    try {
        const url = `${baseUrl}?email=${email}&key=${key}&param=${param}&bdate=${bdate}&edate=${edate}&state=${stateCode}`;
          console.log(url)
      const response = await axios.get(url);
        if (response.data.Data) {
            console.log(`Data for state ${stateCode} retrieved successfully.`);
            return response.data.Data;
        } else {
            console.log(`No data for state ${stateCode}`);
            return null;
        }
    } catch (error) {
        console.error(`Failed to retrieve data for state ${stateCode}:`, error.response ? error.response.status : error.message);
        return null;
    }
}

// Function to save data to JSON
async function saveToCSV() {
   
    function delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    
    // Run the requests concurrently with staggered starts, each delayed by 2 seconds
    const allPromises = stateCodes.map((stateCode, index) =>
        delay(1000 * index).then(() => queryStateData(stateCode.toString().padStart(2, '0')))
    );
    
    let allData = [];
    for (const promise of allPromises) {
        const data = await promise;
        if (data) {
            console.log(data.length);
            for (var i = 0; i < data.length; i++){
                if (data[i].pollutant_standard == "PM25 24-hour 2024"){
                    var point = {
                        latitude: data[i].latitude,
                        longitude: data[i].longitude,
                        date_local: data[i].date_local,
                        arithmetic_mean: data[i].arithmetic_mean,
                        poc: data[i].poc
                    }
                    allData.push(point);
                }
            }
            
        }
    }
    
    if (allData.length > 0) {

        fs.writeFileSync('output.json', JSON.stringify(allData, null, 2), 'utf8');
        console.log(`Data successfully saved to output.json`);
    } else {
        console.log("No data available to save.");
    }
}

// Execute the save function
saveToCSV();

