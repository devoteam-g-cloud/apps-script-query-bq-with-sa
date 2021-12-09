/*
 * Resources > Libraries > OAuth2
 */
// Private key and client email of the service account.
var PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----\n!FAKE!vgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQD8av+nWrUbIEAJ\nWZt301pv4SkLgRxWY4B7wrYxcBD3CXD/jlCPHnlEoJvyEdQfGQEUrQZeHVxDlEd9\nHhKnDEVEKqFx6XLx2HyhYr9lPFPDU0tLhBY1aKqURw650IBhjUPVlW0qqJWLXZUX\nuu7SuoYh32o9Ij+rI0bOGRfap94VShnf23yrRyfo4juhGCFdCPLvfZObZxx7wpCn\nKhodUAchcf0yCn+z7VVCZtUMaTApFcx9Eu0xfC/BQNKcrn4DZEv4fXtDAzGkW59t\nuNStyjd/9v7NSsbYMM5LOh+aTqo0xSI06pkW20NVoRCIJs+3ybpYVZfMS0SIeC+j\nJP3/TrJFAgMBAAECggEAHglDc7AlBd3QEOzDRb52MAL8hdxuuU7szo1MbdSWiDbs\n5dQS/PzU1FhCRktd0PeCO+oc7F0VCgueeCJ4eL1KjsJ95JaxcVrf9bpQ6SSFGSST\nvKAynldemPddprnLVRBgxo0Z6O4XoXZY0/KQEa5VYLl7pFurdNtlRQgSYRgBosmq\ni8td+e2WGF9r21UJuYobTkVYVxIEF0W0oR4pY3ftu69/fzeYt3XIzxcBOGyNjans\nazajxrRelWa1GN5njwFAaAzUhmCYyHwzXQZ4/s5BHrf7ujQhhYRPT3DngpmC3Kbo\ncMDGyFLIW7micQSHvvd4RNkmRQmk7lXuz6iX8O+qPwKBgQD/qSXQYwdWtUzGrPdq\nkiHj+0I4xzSZgyJMpDTmEvElsDlyjGdmUae1a806YsWk9pEx893ghwTcZzRHQJ9P\nGUWuU1b8PA17r85lYwSFbBK8QiSHNBtEofK/M1vfpToE3bA8HGqK3djnp2ACUUkN\njSOO9kjhgtIMfF3A0jd72et4EwKBgQD8wL/S8Un58OEQ32riZoYPaumL9P78D1Uc\npD4tVu3E6XlvKfV/JSowUDJahXibY+gdj0twDeCbsXUQpoKv1BUl7qRRp1kBs1NG\no9Bka75HbNwN7DWS/3n1CXM8aXN3qAHfZMyOq7htGFWRGzXn/+bIBa16Qct75qb+\nFfc4yySnRwKBgH5dk8Q5fmIcQLOewLgbPMcb5PJ9fAE4sNZi/4aM7EatoOd+gIkx\naQyiGRF3yqsr+D5RhGS7N+rc1Bk31sZY/nkY1lDcOendzs2MMKrl7SjCJJ3zYlr9\nFSfuccXMPC41iStc/EhhORnfP/RFSy94iI/cKc/VQo9LVWe+QBBmeR2jAoGBAJ+b\ndEbXufhMhUOrYgqP5W5M/Cg2UOKJKB0LFnx5HXKwYM+y9mUQ6gBSVEnXaR7vSv+H\nooKjMMgAmsUl8PbpfWVTuo4ZmFo7jIXaIlmXtDPUaW2dUQN/strq2cs3aAQSowQX\n2hjqFW9G45nbTgIwFeMPusmuTLn0IFXpNxG1LnM3AoGBANB68zR59DT1E5J8Di6c\nwlRulabyLllLcewNHrv8RgwT1/UCaIfeZKE7JdsDjap+s+e4Uiws0OwfBMNtC2tN\nxvP4+ikrgS3oXLyyCsni9ZwTVwKrBxGVOoRqdeYpViIuQfkfzEazwBB6uOHP4lbk\n24U0AwapGHfbNob2wW2n!FAKE!\n-----END PRIVATE KEY-----\n";
var CLIENT_EMAIL = 'bigquery@sandbox-hzimmermann.iam.gserviceaccount.com';

// Email address of the user to impersonate.
// var USER_EMAIL = '...';

/**
 * Authorizes and makes a request to the Google BigQuery API.
 */
function run() {
  var service = getService();
  if (service.hasAccess()) { 
    var postUrl = 'https://bigquery.googleapis.com/bigquery/v2/projects/sandbox-hzimmermann/queries';
    var data = {
      'query': 'SELECT unique_key, date FROM `bigquery-public-data.chicago_crime.crime` LIMIT 1000001',
      'useLegacySql': false
    };
    var options = {
      'method' : 'post',
      'contentType': 'application/json',
      // Convert the JavaScript object to a JSON string.
      'payload' : JSON.stringify(data),
      'headers': {
        'Authorization': 'Bearer ' + service.getAccessToken()
      }
    };
    
    // call endpoint
    var response = UrlFetchApp.fetch(postUrl, options);
    var result = JSON.parse(response.getContentText());
    
    // process the repsonse
    // var rows = data.rows;
    var nextPageToken = "";
    var jobId = result.jobReference.jobId;  
    var jobComplete = result.jobComplete;
    
    // set up variables
    var allData = [];
    var firstCall = true;
    
      while (jobComplete == false || nextPageToken || firstCall) {
        if (firstCall){
          nextPageToken = "";
        }
        
        var url = "https://bigquery.googleapis.com/bigquery/v2/projects/sandbox-hzimmermann/queries/" + jobId + "?pageToken="+nextPageToken;
        var options = {
          'method' : 'get',
          'headers': {
            'Authorization': 'Bearer ' + service.getAccessToken()
          }
        };
        response = UrlFetchApp.fetch(url, options);
        result = JSON.parse(response.getContentText());
        jobComplete = result.jobComplete;
        if (jobComplete) {
          firstCall = false;
        } else {
          console.log('Waiting 3 seconds...');
          Utilities.sleep(3000);
        }
        var rows = result.rows;
        nextPageToken = result.pageToken;
        console.log('nextPageToken');
        console.log(nextPageToken);
        
        if (rows == null) {
          console.log('No rows returned.');
        } else {
          // Parse and append the results.
          var data = new Array(rows.length);
          for (var i = 0; i < rows.length; i++) {
            var cols = rows[i].f;
            data[i] = new Array(cols.length);
            for (var j = 0; j < cols.length; j++) {
              data[i][j] = cols[j].v;     
            }
          }
          allData = allData.concat(data);
          console.log('allData.length');
          console.log(allData.length);
          console.log('-------------------------');
        }  
      }
    console.log('Done.');
    console.log('The last row of data:');
    console.log(allData[allData.length -1]);
    return allData;
  } else {
    Logger.log(service.getLastError());
  }
}

/**
 * Reset the authorization state, so that it can be re-tested.
 */
function reset() {
  getService().reset();
}

/**
 * Configures the service.
 */
function getService() {
  return OAuth2.createService('GoogleBigQuery')
      // Set the endpoint URL.
      .setTokenUrl('https://oauth2.googleapis.com/token')

      // Set the private key and issuer.
      .setPrivateKey(PRIVATE_KEY)
      .setIssuer(CLIENT_EMAIL)

      // Set the name of the user to impersonate. This will only work for
      // Google Apps for Work/EDU accounts whose admin has setup domain-wide
      // delegation:
      // https://developers.google.com/identity/protocols/OAuth2ServiceAccount#delegatingauthority
      // .setSubject(USER_EMAIL)

      // Set the property store where authorized tokens should be persisted.
      .setPropertyStore(PropertiesService.getScriptProperties())

      // Set the scope. This must match one of the scopes configured during the
      // setup of domain-wide delegation.
      .setScope('https://www.googleapis.com/auth/bigquery');
}