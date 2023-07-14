[![REUSE status](https://api.reuse.software/badge/github.com/SAP-samples/analytics-cloud-export-api-wrapper)](https://api.reuse.software/info/github.com/SAP-samples/analytics-cloud-export-api-wrapper)

# sacapi - The SAP Analytics Cloud Model REST API Wrapper

sacapi is a Python wrapper for SAP Analytics Cloud’s (SAC) model import and [export](https://help.sap.com/docs/SAP_ANALYTICS_CLOUD/14cac91febef464dbb1efce20e3f1613/db62fd76514b48f8b71d695360320f4a.html) APIs.  It has two aims.  Firstly, it demonstrates the basics for working with the SAC model  APIs and incorporating them into code.  Secondly, it is running code that can be used to access data from SAC in Jupyter notebooks and other Python code.  It does not try to be an all-encompassing wrapper for all API endpoints and is specifically focused on the data science use case.  It allows the user to read the catalog of models, filter for and select models, read master and fact data from selected models and write fact data to models.  It excludes the master data management use case and leaves out access (read or write) to currency rate tables and public dimensions.

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

Everything is controlled through an SACConnection object.  To create this object, you'll need the tenant name and data center code for your SAC tenant.

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


## Detailed usage and information about REST API interaction


## Known Issues

Only 2 legged OAuth is supported.

## How to obtain support
[Create an issue](https://github.com/SAP-samples/analytics-cloud-export-api-wrapper/issues) in this repository if you find a bug or have questions about the content.
 
For additional support, join the [SAP](https://community.sap.com/topics/cloud-analytics)[ Analytics Community](https://community.sap.com/topics/cloud-analytics)](https://community.sap.com/topics/cloud-analytics), where you can find discussions and additional resources.  If your question is not already answered, you can[ask a question in SAP Community](https://answers.sap.com/questions/ask.html?primaryTagId=67838200100800006884&additionalTagId=73554900100800000562&additionalTagId=819703369010316911100650199149950&additionalTagId=3f33380c-8914-4b7a-af00-0e9a70705a32&additionalTagId=73555000100800001621&additionalTagId=2221d1b0-d759-4b24-9333-f72da4d263da&additionalTagId=3ecbe2ed-7fe9-4831-924a-77987d1a4259). 

## Contributing
If you wish to contribute code, offer fixes or improvements, please send a pull request. Due to legal reasons, contributors will be asked to accept a DCO when they create the first pull request to this project. This happens in an automated fashion during the submission process. SAP uses [the standard DCO text of the Linux Foundation](https://developercertificate.org/).

## License
Copyright (c) 2022 SAP SE or an SAP affiliate company. All rights reserved. This project is licensed under the Apache Software License, version 2.0 except as noted otherwise in the [LICENSE](https://raw.githubusercontent.com/SAP-samples/analytics-cloud-export-api-wrapper/main/LICENSES/Apache-2.0.txt?token=GHSAT0AAAAAAB2MAUL26SOZQOB4VIRO3ZNCY5SU5VQ) file.
