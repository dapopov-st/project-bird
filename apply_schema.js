const fs = require('fs');
const csv = require('csv-parser');
const { parse } = require('json2csv');

const schema = [
    {"name": "speciesCode", "type": "STRING", "mode": "REQUIRED"},
    {"name": "comName", "type": "STRING", "mode": "REQUIRED"},
    {"name": "sciName", "type": "STRING", "mode": "REQUIRED"},
    {"name": "locId", "type": "STRING", "mode": "REQUIRED"},
    {"name": "locName", "type": "STRING", "mode": "REQUIRED"},
    {"name": "obsDt", "type": "STRING", "mode": "REQUIRED"},
    {"name": "howMany", "type": "STRING", "mode": "REQUIRED"},
    {"name": "lat", "type": "STRING", "mode": "REQUIRED"},
    {"name": "lng", "type": "STRING", "mode": "REQUIRED"},
    {"name": "obsValid", "type": "STRING", "mode": "REQUIRED"},
    {"name": "obsReviewed", "type": "STRING", "mode": "REQUIRED"},
    {"name": "locationPrivate", "type": "STRING", "mode": "REQUIRED"},
    {"name": "subId", "type": "STRING", "mode": "REQUIRED"}
];

function parseLocationString(locationString) {
    const regexWithLatLong = /^(.*),\s*([+-]?\d+\.\d+),\s*([+-]?\d+\.\d+)$/;
    const matchWithLatLong = locationString.match(regexWithLatLong);

    if (matchWithLatLong) {
        return {
            locationName: matchWithLatLong[1].trim(),
            lat: parseFloat(matchWithLatLong[2]),
            long: parseFloat(matchWithLatLong[3])
        };
    } else {
        return {
            locationName: locationString.trim(),
            lat: null,
            long: null
        };
    }
}

function transform(row) {
    const obj = {};

    for (let i = 0; i < schema.length; i++) {
        obj[schema[i].name] = row[schema[i].name];
    }

    // Check if lat and lng are already present
    if (!obj['lat'] || !obj['lng']) {
        // Parse the location string if lat and lng are not present
        const locationString = obj['locName'];
        try {
            const parsedLocation = parseLocationString(locationString);
            obj['locName'] = parsedLocation.locationName;
            obj['lat'] = parsedLocation.lat || obj['lat'];
            obj['lng'] = parsedLocation.long || obj['lng'];
        } catch (error) {
            console.error(`Error parsing location string: ${locationString}`);
        }
    }

    return obj;
}

async function processFile(inputFilePath, outputFilePath) {
    const results = [];

    fs.createReadStream(inputFilePath)
        .pipe(csv())
        .on('data', (data) => results.push(transform(data)))
        .on('end', () => {
            const csvData = parse(results, { fields: schema.map(field => field.name) });
            fs.writeFileSync(outputFilePath, csvData);
            console.log('CSV file successfully processed');
        });
}

processFile('recent_observations.csv', 'output_data.csv');