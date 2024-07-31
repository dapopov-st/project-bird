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

function parseCSVLine(line) {
    const values = [];
    let current = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
        const char = line[i];

        if (char === '"' && (i === 0 || line[i - 1] !== '\\')) {
            inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
            values.push(current);
            current = '';
        } else {
            current += char;
        }
    }
    values.push(current);

    return values;
}

function transform(line) {
    var values = parseCSVLine(line);
    var obj = {};
    
    for (let i = 0; i < schema.length; i++) {
        obj[schema[i].name] = values[i];
    }
    
    var jsonString = JSON.stringify(obj);
    return jsonString;
}