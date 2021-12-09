# CONTEXT
Stand-alone file.
It lets you send queries to BigQuery and read the responses.  
It uses the OAuth2 library to authenticate to BQ via a Service Account.  
Easily adaptable to call other API methods using SA authentication.  

# SET UP
1. Create or use an existing service account that has IAM rights over BigQuery. To have the main function of our file work (Execute a SQL query on BigQuery) the SA should have the BigQuery User role.
2. Generate it's credential file as a json.  
3. Copy the query-bq-with-sa.gs in your Apps Script project.   
4. In the menu Resources > Libraries: enable the OAuth2 library.   
5. With the information from the credential file replace the value of the PRIVATE_KEY and CLIENT_EMAIL variables in the query-bq-with-sa.gs file.  
6. You can execute the run() function to make sure it works.  
7. Adapt the code for your needs.  