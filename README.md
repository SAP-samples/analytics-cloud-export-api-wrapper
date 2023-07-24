[![REUSE status](https://api.reuse.software/badge/github.com/SAP-samples/analytics-cloud-export-api-wrapper)](https://api.reuse.software/info/github.com/SAP-samples/analytics-cloud-export-api-wrapper)

# sacapi - The SAP Analytics Cloud Model REST API Wrapper

sacapi is a Python wrapper for SAP Analytics Cloud’s (SAC) model import and [export](https://help.sap.com/docs/SAP_ANALYTICS_CLOUD/14cac91febef464dbb1efce20e3f1613/db62fd76514b48f8b71d695360320f4a.html) APIs.  It has two aims.  Firstly, it demonstrates the basics for working with the SAC model  APIs and incorporating them into code.  Secondly, it is running code that can be used to access data from SAC in Jupyter Notebooks and other Python code.  It does not try to be an all-encompassing wrapper for all API endpoints and is specifically focused on the data science use case.  It allows the user to read the catalog of models, filter for and select models, read master and fact data from selected models and write fact data to models.  It excludes the master data management use case and leaves out access (read or write) to currency rate tables and public dimensions.

SAC's export REST API is a [Cloud Data Integration (CDI)](https://help.sap.com/docs/HANA_SMART_DATA_INTEGRATION/7952ef28a6914997abc01745fef1b607/233ff3514ff74106937adc39db9be0dd.html) interface and is fully [OData](https://www.odata.org/) compliant.  SAC's import REST API is a bespoke REST interface.  sacapi wraps the API endpoints for these two REST interfaces most often used in analysis and data science workflows.  

## Requirements

Python 3.7 or higher installed
An SAC tenant with [authentication configured for an application](https://help.sap.com/docs/SAP_ANALYTICS_CLOUD/14cac91febef464dbb1efce20e3f1613/0c1fb5e6ef1f46acb83771070084f124.html).  sacapi only supports two-legged OAuth workflows and not three-legged authentication, hence the application authentication requirement.  


## Download and Installation

```python
pip install sacapi
```

-or-

Copy and paste the code into your Python script, or download the package and place it in a location that is in your PYTHONPATH.


## Quickstart

Everything is controlled through a SACConnection object.  To create this object, you'll need the tenant name and data center code for your SAC tenant.

```python
sac = SACConnection(<tenantName>, <dataCenter>)
```

E.g. to connect to the appdesign demo tenant, in the eu10 data center, the command would be:
```python
sac = SACConnection("appdesign", "eu10")
```

To connect, execute the SACConnection's connect() method, with the OAuth clientID and appSecret.
```python
sac.connect(<clientID>, <appSecret>)
```

To work with a model, you'll need a ModelMetadata object, for that model.  To get it, call the getModelMetadata() method of the SACConnection object, along with the technical ID of the model.  


```python
md = sac.getModelMetadata(<modelTechnicalID>)
```

To get the model fact data, call the SACConnection object's getFactData(), with the ModelMetadata object.

```python
fd = sac.getFactData(md)
```



```python
sac.upload(md, <uploadData>)
```


## sacapi Usage
Whether reading from or writing to SAC data models, the workflow follows a broadly similar three-step process.  

You need to connect to and authenticate to the tenant.
You need to fetch the metadata for any models that you want to work with.
Read from the [OData export](https://help.sap.com/docs/SAP_ANALYTICS_CLOUD/14cac91febef464dbb1efce20e3f1613/db62fd76514b48f8b71d695360320f4a.html) API, or write to the REST import API, as desired.

There are [diagrams showing which methods of **sacapi** touch which endpoints](https://github.com/SAP-samples/analytics-cloud-export-api-wrapper/blob/main/docs/sacapi_rest_api_usage.md) in the SAC OData Export API and REST Import API, respectively.



## Before you start

**sacapi** needs four pieces of information to successfully connect and authenticate to a SAC tenant:
1. Tenant Name
2. Data Center
3. OAuth Client ID
4. OAuth App Secret

You can get the tenant name from the URL that you use to access your SAC tenant.  The pattern for SAC tenant URLs is https://**<TenantName>**.**<DataCenter>**.sapanalytics.cloud/.  E.g. take the following tenant URL

'''
https://appdesign.eu10.sapanalytics.cloud/
'''

The tenant name is "appdesign"
The data center is "eu10"

When you connect to a SAC tenant, you will first need to acquire an [Open](https://oauth.net/) Authorization (OAuth](https://oauth.net/)) access token.  **sacapi** will connect to the SAC tenant and acquire the Open Authorization (OAuth) access token for you but only supports 2-legged OAuth, and not 3-legged.  To use 2-legged OAuth with SAC, you'll need to need to [configure "App Integration"](https://blogs.sap.com/2023/01/09/data-export-api-lets-do-some-app-integration/), so that you have a client ID and app secret.  **sacapi** is designed under the presumption that it will run in a script, or in short-lived interactive shell sessions.  As such, it has no provision for managing and renewing long-lived OAuth tokens.  If you need this capability, you'll have to fork it and add that ability.



## Connecting and Authenticating

Whenever you work with a SAC tenant, you need an SACConnection object, for that tenant.  This object will manage the connection info, OAuth token and model metadata for any models that you are working with.  SACConnection objects have a 1:n relationship to the models that it manages, so you only need one SACConnection object per tenant, even if you work with multiple models.  The init method of SACConnection takes the tenant name and data center as mandatory positional arguments.  It returns a SACConnection object.

```python
sac = SACConnection(<tenantName>, <dataCenter>)
```

Once you have your SACConnection object, you need to authenticate.  The SACConnection class has a connect() method.  connect() takes to OAuth client ID and App Secret as mandatory positional parameters.  This method will connect to SAC, acquire the OAuth token and read the model catalog; both of which are respectively stored in the *accessToken* and *providerLookup* instance variables.

```python
sac.connect(<clientID>, <appSecret>)
```

connect() wraps the getAccessToken() and getProviders() methods.  You generally don't need to call them directly, but if you wanted to intervene:
1. getAccessToken() is called first, with client ID and App Secret as manadatory positional parameters.  It fetches the token and sets is as the *accessToken* instance variable.  
2. getProviders() takes no arguments.  It connects to the /dataexport/administration/Namespaces(NamespaceID='sac')/Providers/ endpoint and reads the list of available models, storing it in the *providerLookup* instance variable.  Provider -vs- Model.  These two words are synonyms in the SAC APIs.  The OData Export API specifically uses the word *provider*, to maintain consistency with the [Cloud Data Integration (CDI)](https://help.sap.com/docs/HANA_SMART_DATA_INTEGRATION/7952ef28a6914997abc01745fef1b607/233ff3514ff74106937adc39db9be0dd.html) flavor of [OData](https://www.odata.org/) that it implements.  The REST import API is a custom REST interface and is not constrained by CDI terminology.  As such, *model* is more comprehensible.

### The providerLookup instance variable

*providerLookup* is a dictionary, which assists the user in finding the internal ID of a given model.  In the SAC UI, users see the text name (description) of the model.  There is a unique internal ID, which SAC uses to refer to the model.  For many API endpoints (and therefore for the corresponding methods), this internal ID is used to refer to the model.  In *providerLookup* the description is the key and the modelID is the value.  If you have a large number of models in your tenant, you can use the searchProviders() method.  It takes a search string parameter and returns a dictionary object, containing all of the entries with that search substring in the description.  This might be easier to handle.



## Model Metadata

When you want to work with a specific model, you'll need to acquire its metadata,  This happens in three steps:
1. Get the modelID, from 
2. Fetch and parse the model's [OData EDMX](https://www.odata.org/documentation/odata-version-2-0/overview/) document, to get the basic structure of the model; what columns are in the model, which are dimensions and which are measures.
3. For each of the dimensions, fetch the dimension master data.

To do all of this, use the getModelMetadata() method.  It takes a single, mandatory parameter; modelTechnicalID and returns a **ModelMetadata** object.  This **ModelMetadata** object will be your proxy for setting OData filter parameters for export, acquiring fact and audit data and writing data back to the import APIs.  

```python
md = sac.getModelMetadata(<modelTechnicalID>)
```

The instance variables of **ModelMetadata** hold information about which columns are:
* dimensions
* dateDimensions
* measures
* accounts

Other instance variables are also used to keep track of which versions are available in the model, what the current default targetVersion is (for operations specific to a version)
* versions
* targetVersion
* mapping




## OData filters in Export

OData supports query filters.  Before you downlaod any data, you can manage the OData filters, for that model.   **sacapi** supports logical filters, string filters and handcrafted string filters; stored for the length of the session.  For convenience, the valid filter operators are stored in the class variables *filterOperators* and *filterStringOperations*, in **SACConnection**.   **SACConnection** addLogicalFilter() and addLogicalFilter() (the so-called "fast filters") methods both take four mandatory parameters; modelID, columnName, filterValue, and operator.


```python
sac.addLogicalFilter(<modelID>, columnName, filterValue, operator)
```

```python
sac.addStringFilter(<modelID>, columnName, filterValue, operator)
```

You can add additional filters, thereby stacking them.  There is always an implicit "AND" presumed.

```python
sac.addStringFilter(<modelID>, "Region", "Pacific", sac.filterStringOperations.STARTS_WITH)
sac.addLogicalFilter(<modelID>, "NationalParkUnitType", "National Park", sac.filterOperators.EQUAL)
sac.addLogicalFilter(<modelID>, "Date", "202105", sac.filterOperators.EQUAL)
```

You can order returned fact data, by a specified column.

The setFilterOrderBy() method 
```python
sac.setFilterOrderBy(<modelID>, orderByCol, ascDesc)
```

The available options for ascDesc are "asc" and "desc".

You can also override these "fast filters", and manually set an OData filter query.  If there is an override manual filter present, it will always be used, instead of whatever fast filters may be applied.

```python
sac.setParamOverride(<modelID>, moValue)
```

*moValue* is the manual override value, and is a string containing an OData filter.  E.g.

```python
sac.setParamOverride(<modelID>, "$top=5&$orderby=State asc&$filter=startswith(Region,'Pacific') and State ne 'WA'")
```

If you want to remove this override and revert to the fast filters (if any exist), clearParamOverride() will do this for you.
```python
sac.clearParamOverride(<modelID>)
```


### Logical Filters
* SACConnection.filterOperators.EQUAL
* SACConnection.filterOperators.NOT_EQUAL
* SACConnection.filterOperators.GREATER_THAN
* SACConnection.filterOperators.LESS_THAN
* SACConnection.filterOperators.GREATER_THAN_OR_EQUAL
* SACConnection.filterOperators.LESS_THAN_OR_EQUAL

### String Filters
* SACConnection.filterStringOperations.CONTAINS
* SACConnection.filterStringOperations.STARTS_WITH
* SACConnection.filterStringOperations.ENDS_WITH
 


## Getting Fact Data (Export)


To get the model fact data, call the getFactData() method.  It takes the model ModelMetadata object as its only parameter.  It returns a list of dictionaries, with the column names as keys and cell values as values.  This includes the version column, which is normally hidden.  The returned data can be immediately converted into a [Pandas](https://pandas.pydata.org/) dataframe.

```python
fd = sac.getFactData(md)
```

An example return
```
[
    {
        'Version': 'public.Actual', 
        'Date': '202105', 
        'NationalPark': 'YOSE', 
        'Region': 'Pacific West ', 
        'State': 'CA', 
        'AddedDate': '202306', 
        'NationalParkUnitType': 'National Park', 
        'Visitors': 67284
    }
]
```



## Import Specific Methods

For importing, there are three principal methods.  You can:
Set the mapping, if needed, between the uploaded data and the model columns.  The mapping is maintained as a dictionary instance variable, on the **ModelMetadata** object.
Validate the mapping, which the **ModelMetadata** currently has.  
Upload fact data

Af a column in your dataset has the same name as a column in the model, it will automatically be mapped.  If you have a column that needs to be mapped, to match the model, the setMapping() method, in the **ModelMetadata** class will do this for you.

The pattern is setMapping(<datasetColumnName>, <modelColumnName>)

E.g.

```python
md.setMapping("NationalPark", "ParkID")
```

The validateMapping() method, in the **ModelMetadata** class validates the current mapping.  It will tell you which columns in the model currently have nothing mapped to them and which columns in the dataset are not mapped to anything in the model.  The validateMapping() method takes the dataset, or the first column of the dataset as its single, required parameter.  It will not upload the dataset, only check its mapping.  It will return a dict, containing this information.

E.g.

```python
unmappedCols = md.validateMapping([{'Date': '202105', 'AddedDate': '202307', 'ParkID': 'YOSE', 'Region': 'Pacific West ', 'State': 'CA', 'NationalParkUnitType': 'National Park', 'Recreational Visitors': 67284}])
```

-OR-

```python
unmappedCols = md.validateMapping({'Date': '202105', 'AddedDate': '202307', 'ParkID': 'YOSE', 'Region': 'Pacific West ', 'State': 'CA', 'NationalParkUnitType': 'National Park', 'Recreational Visitors': 67284})
```

To upload the data, use the upload() method, on the **SACConnection** class.  It takes the **ModelMetadata** object and the uploaded fact data as parameters.

```python
sac.upload(md, <uploadData>)
```



## Known Issues

Only 2 legged OAuth is supported.
Renewal of OAth token sessions is not supported.  It is expected that scripts using sacapi run their course during the timeframe of the initial token.


## How to obtain support
[Create an issue](https://github.com/SAP-samples/analytics-cloud-export-api-wrapper/issues) in this repository if you find a bug or have questions about the content.
 
For additional support, join the [SAP](https://community.sap.com/topics/cloud-analytics)[ Analytics Community](https://community.sap.com/topics/cloud-analytics)](https://community.sap.com/topics/cloud-analytics), where you can find discussions and additional resources.  If your question is not already answered, you can[ask a question in SAP Community](https://answers.sap.com/questions/ask.html?primaryTagId=67838200100800006884&additionalTagId=73554900100800000562&additionalTagId=819703369010316911100650199149950&additionalTagId=3f33380c-8914-4b7a-af00-0e9a70705a32&additionalTagId=73555000100800001621&additionalTagId=2221d1b0-d759-4b24-9333-f72da4d263da&additionalTagId=3ecbe2ed-7fe9-4831-924a-77987d1a4259). 

## Contributing
If you wish to contribute code, offer fixes or improvements, please send a pull request. Due to legal reasons, contributors will be asked to accept a DCO when they create the first pull request to this project. This happens in an automated fashion during the submission process. SAP uses [the standard DCO text of the Linux Foundation](https://developercertificate.org/).

## License
Copyright (c) 2022 SAP SE or an SAP affiliate company. All rights reserved. This project is licensed under the Apache Software License, version 2.0 except as noted otherwise in the [LICENSE](https://raw.githubusercontent.com/SAP-samples/analytics-cloud-export-api-wrapper/main/LICENSES/Apache-2.0.txt?token=GHSAT0AAAAAAB2MAUL26SOZQOB4VIRO3ZNCY5SU5VQ) file.

<!--
SPDX-FileCopyrightText: SAP SE or an SAP affiliate company <david.stocker@sap.com>
SPDX-License-Identifier: Apache-2.0
-->