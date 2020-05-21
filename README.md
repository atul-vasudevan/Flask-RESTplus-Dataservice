# Flask-RESTplus-Dataservice
Develop a Flask-Restplus data service that allows a client to read and store some publicly available economic indicator data for countries around the world, and allow the consumers to access the data through a REST API.

The source URL: http://api.worldbank.org/v2/
Documentations on API Call Structure:
https://datahelpdesk.worldbank.org/knowledgebase/articles/898581-api-basic-call-structure
The World Bank indicators API provides 50 year of data over more than 1500 indicators for countries around the world.

List of indicators can be found at: http://api.w o rldbank.org/v2/indicators
List of countries can be found at: http://api.worldbank.org/v2/countries

# Tasks
- Import a collection from the data service
- Deleting a collection with the data service
- Retrieve the list of available collections
- Retrieve a collection
- Retrieve economic indicator value for given country and a year
- Retrieve top/bottom economic indicator values for a given year
