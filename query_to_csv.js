const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const functions = require('@google-cloud/functions-framework');
const {Parser} = require('json2csv');

// Register an HTTP function with the Functions Framework that will be executed
// when you make an HTTP request to the deployed function's endpoint.
functions.http('query_to_csv', async (req, res) => {
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

    // Encode the CSV data to Base64
    const base64Csv = Buffer.from(csv).toString('base64');


  // Create the response object
  const response = {
    openaiFileResponse: [
    {
      name: 'query_results.csv',
      mime_type: 'text/csv',
      content: base64Csv
    }
    ]
  };

  // Send the response
  return res.status(200).send(response);    

  } catch (err) {
    console.error(err);
    res.status(500).send(`Error querying BigQuery: ${err}`);
  }    
});