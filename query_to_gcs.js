const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const functions = require('@google-cloud/functions-framework');
const {Parser} = require('json2csv');
const { Storage } = require('@google-cloud/storage');
const storage = new Storage();


// Register an HTTP function with the Functions Framework that will be executed
// when you make an HTTP request to the deployed function's endpoint.
functions.http('query_to_file', async (req, res) => {
  const sqlQuery = req.body.query;
  //const sqlQuery = "SELECT * FROM `enhanced-storm-404515.Flights.flights_nyc_jan2013` LIMIT 10";

  if (!sqlQuery) {
    res.status(400).send('No query provided in the request body.');
    return;
  }

  const options = {
    query: sqlQuery,
    // Location must match that of the dataset(s) referenced in the query.
    location: 'US',
  };

  // Execute the query
  try {
    const [rows] = await bigquery.query(options);            
    
    // Convert the query result to CSV
    const parser = new Parser();
    const csv = parser.parse(rows);    

    // Define your bucket and file name
    const bucketName = 'girish-bucket-oai';
    const fileName = 'query_results.csv';
    
    // Create a file object
    const file = storage.bucket(bucketName).file(fileName);

    // Upload the CSV data to Google Cloud Storage
    await file.save(csv, {
      contentType: 'text/csv',
      resumable: false
    });

    // Construct the URL for the file
    const fileUrl = `https://storage.cloud.google.com/${bucketName}/${fileName}`;

    // Create the response object
    const response = {
      openaiFileResponse: [fileUrl]
    };
    
    // Send the response
    return res.status(200).send(response);    

  } catch (err) {
    console.error(err);
    res.status(500).send(`Error querying BigQuery: ${err}`);
  }    
});