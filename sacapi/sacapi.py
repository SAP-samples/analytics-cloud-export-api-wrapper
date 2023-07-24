import json
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from xml.dom import minidom

class RESTError(ValueError):
    pass

class OAuthError(ValueError):
    pass

class RESTParamsError(ValueError):
    pass

class MissingCSRFTokenError(ValueError):
    pass

class UnmatchedColumnsError(ValueError):
    pass

class InvalidRowsError(ValueError):
    pass

class JobDeleteFailure(ValueError):
    pass


class FilterOperators(object):
    EQUAL = "eq"
    NOT_EQUAL = "ne"
    GREATER_THAN = "gt"
    LESS_THAN = "lt"
    GREATER_THAN_OR_EQUAL = "ge"
    LESS_THAN_OR_EQUAL = "le"


class StringFilters(object):
    CONTAINS = "contains"
    STARTS_WITH = "startswith"
    ENDS_WITH = "endswith"

class FilterLogicGateSymbols(object):
    LG_AND = "and"
    LG_OR = "or"
    LG_NOT = "not"

class UpdatePolicy(object):
    UPDATE = "Update"
    CLEAN_AND_REPLACE = "CleanAndReplace"



logicGate = FilterLogicGateSymbols()


class SACProvider(object):
    def __init__(self, providerID, providerName, description, serviceURL):
        self.providerID = providerID
        self.providerName = providerName
        self.namespace = "sap"
        self.description = description
        self.serviceURL = serviceURL

class ModelMetadata(object):
    dimensions = {}
    dateDimensions = {}
    measures = []
    accounts = {}
    versions = {}
    targetVersion = None
    mapping = {}

    def __init__(self, providerID):
        self.modelID = providerID

    def initializeMapping(self):
        firstKey = list(self.versions.keys())[0]
        firstKeyValue = self.versions[firstKey]
        self.targetVersion = list(firstKeyValue.keys())[0]

        #Initialize the mapping as if it were 1:1, essentially unmapped.
        columnList = []
        columnList.extend(self.dimensions.keys())
        columnList.extend(self.dateDimensions.keys())
        columnList.extend(self.accounts.keys())
        columnList.extend(self.measures)
        for column in columnList:
            self.mapping [column] = column

    def setMapping(self, modelCol, sourceCol):
        try:
            self.mapping[modelCol] = sourceCol
        except KeyError as oa:
            errorMsg = "Model column %s does not exist in model %s" % (modelCol, self.modelID)
            raise ValueError(errorMsg)
        except Exception as e:
            errorMsg = "Unknown error during mapping validation"
            errorMsg = "%s  %s" % (errorMsg, e.error)
            raise Exception(errorMsg)


    def validateMapping(self, sampleTuple, inheritTargetVersion = True):
        sampleTupleNestedList = False
        unmatchedModelCols = []
        unmatchedImportCols = []

        try:
            if type(sampleTuple) is list:
                #sampleTuple was provided as part of a list.  Take the first element
                sampleTuple =  sampleTuple[0]
                sampleTupleNestedList = True

            if type(sampleTuple) is dict:
                mappingValues = self.mapping.values()

                # Check which model columns have no mappings from the sample tuple
                for currMofColKey in self.mapping.keys():
                    currMofColVal = self.mapping[currMofColKey]
                    if currMofColVal not in sampleTuple.keys():
                        unmatchedModelCols.append(currMofColKey)

                # check which
                for currImpColKey in sampleTuple.keys():
                    if currImpColKey == 'Version':
                        if inheritTargetVersion is True:
                            self.targetVersion = sampleTuple[currImpColKey]
                    else:
                        if currImpColKey not in mappingValues:
                            unmatchedImportCols.append(currImpColKey)

                returnDict = {"unmatchedModelColumns": unmatchedModelCols, "unmatchedImportCols": unmatchedImportCols}
                return returnDict
            else:
                errorMsg = ""
                if sampleTupleNestedList:
                    errorMsg = "validateMapping() was called with a list.  If checkMapping() is called with a list, then it needs to be a list of dicts.  Instead a dict, the first element is of type %s" %(type(sampleTuple))
                else:
                    errorMsg = "validateMapping() must be called with a dict, or list of dicts.  It was instead called with a %s" % (type(sampleTuple))
                raise ValueError(errorMsg)
        except ValueError as oa:
            raise oa
        except Exception as e:
            errorMsg = "Unknown error during mapping validation"
            errorMsg = "%s  %s" %(errorMsg, e.error)
            raise Exception(errorMsg)






class SACConnection(object):
    def __init__(self, tenantName, dataCenter):
        self.tenantName = tenantName
        self.dataCenter = dataCenter
        self.connectionNamespace = "sap"
        self.urlAccessToken = "https://" + tenantName + ".authentication." + dataCenter + ".hana.ondemand.com/oauth/token"
        self.urlExportNamespaces = "https://" + tenantName + "." + dataCenter + ".sapanalytics.cloud/api/v1/dataexport/administration/Namespaces"
        self.urlExportProviders = "https://" + tenantName + "." + dataCenter + ".sapanalytics.cloud/api/v1/dataexport/administration/Namespaces(NamespaceID='sac')/Providers"
        self.urlExportProviderRoot = "https://" + tenantName + "." + dataCenter + ".sapanalytics.cloud/api/v1/dataexport/providers/sac"
        self.urlImportModels = "https://" + tenantName + "." + dataCenter + ".sapanalytics.cloud/api/v1/dataimport/models"
        self.urlImportJobs = "https://" + tenantName + "." + dataCenter + ".sapanalytics.cloud/api/v1/dataimport/jobs"
        self.accessToken = None
        self.httpPostHeader = None
        self.providers = {}
        self.providerLookup = {}
        self.modelMetadata = {}

        #Filters
        self.paramManualOverride = {}
        self.LG_AND = "and"
        self.LG_OR = "or"
        self.LG_NOT = "not"
        self.filterOrderBy = {}
        self.filters = {}
        self.filterLogicGates = {}
        self.filterOperators = FilterOperators()
        self.filterStringOperations = StringFilters()
        self.logicGateOperators = FilterLogicGateSymbols()
        self.updatePolicy = UpdatePolicy()


    def getAccessToken(self, clientID, clientSecret):
        self.clientID = clientID
        self.clientSecret = clientSecret

        try:
            client = BackendApplicationClient(client_id=clientID)
            self.oauth = OAuth2Session(client=client)
            try:
                self.accessToken = self.oauth.fetch_token(token_url=self.urlAccessToken, client_id=clientID, client_secret=clientSecret)
            except Exception as e:
                errorMsg = "Unknown error during token acquisition."
                if e.status_code:
                    errorMsg = "%s  Status code %s from server.  %s" % (errorMsg, e.status_code, e.error)
                else:
                    errorMsg = "%s  %s" % (errorMsg, e.error)
                raise OAuthError(errorMsg)
        except OAuthError as oa:
            raise oa
        except Exception as e:
            errorMsg = "Unknown error during token acquisition."
            if e.status_code:
                errorMsg = "%s  Status code %s from server.  %s" %(errorMsg, e.status_code, e.error)
                raise RESTError(errorMsg)
            else:
                errorMsg = "%s  %s" %(errorMsg, e.error)
                raise Exception(errorMsg)

    def getProviders(self):
        try:
            #Touch the export providers endpoint, to get the catalog of available models
            response = self.oauth.get(self.urlExportProviders)  #note - self.urlImportModels  would return the same thing

            # Touch the import providers endpoint.  It gives mostly the same info as the export providers endpoint (with import, instead of export service urls), but it also gives us the CSRF token
            importResponse = None
            try:
                initialHeaderParams = {"x-csrf-token": "fetch"}
                importResponse = self.oauth.get(self.urlImportModels, headers=initialHeaderParams)
                importCSRFToken = importResponse.headers._store["x-csrf-token"]
                self.httpPostHeader = {"x-csrf-token": importCSRFToken[1]}
                self.csrfTokenStatus = True
            except KeyError:
                importResponse = self.oauth.get(self.urlExportProviderRoot)
                self.csrfTokenStatus = False
                warningMsg = "WARNING.  Failed to connect to %s and fell back on %s, to read the model catalog." % (self.urlImportModels, self.urlExportProviderRoot)
                warningMsg = "%s  No CSRF token is available from this endpoint, so import operations will not be possible." % warningMsg
                print(warningMsg)

            responseJson = json.loads(response.text)
            for provData in responseJson["value"]:
                providerID = provData["ProviderID"]
                providerName = provData["ProviderName"]
                description = provData["Description"]
                serviceURL = provData["ServiceURL"]
                provider = SACProvider(providerID, providerName, description, serviceURL)

                #Add the provider
                self.providers[providerID] = provider

                #Add the provider to the lookup index.  The end user will have access to the providerName, but not the providerID.
                #providerName might not be unique, so be defensive about it...
                if providerName not in self.providerLookup:
                    self.providerLookup[providerName] = providerID
                else:
                    freeSlot = False
                    nNth = 1
                    while freeSlot == False:
                        trialName = "%s (%s)" %(providerName, nNth)
                        if trialName not in self.providerLookup:
                            self.providerLookup[trialName] = providerID
                            freeSlot = True
                        else:
                            nNth = nNth + 1
        except Exception as e:
            errorMsg = "Unknown error during provider (model) calatog read."
            if e.status_code:
                errorMsg = "%s  Status code %s from server.  %s" %(errorMsg, e.status_code, e.error)
                raise RESTError(errorMsg)
            else:
                errorMsg = "%s  %s" %(errorMsg, e.error)
                raise Exception(errorMsg)

    def openLoadJob(self, modelMetadata, factOnly = True, importMethod = "Update"):
        if self.csrfTokenStatus:
            try:
                importType = "/factData"
                if not factOnly:
                    importType = "/masterFactData"

                urlJobCreate = self.urlImportModels + "/" + modelMetadata.modelID + importType
                postBody = json.dumps(modelMetadata.mapping)
                postBody = '{ "Mapping": %s }, "JobSettings": { "importMethod": %s} ' %(postBody, importMethod)
                jobCreationResponse = self.oauth.post(urlJobCreate, headers=self.httpPostHeader, data=postBody)

                responseJson = json.loads(jobCreationResponse.text)
                return responseJson['jobID']

            except Exception as e:
                errorMsg = "Unknown error during load job creation."
                if e.status_code:
                    errorMsg = "%s  Status code %s from server.  %s" % (errorMsg, e.status_code, e.error)
                    raise RESTError(errorMsg)
                else:
                    errorMsg = "%s  %s" % (errorMsg, e.error)
                    raise Exception(errorMsg)
        else:
            errorMsg = "Missing CSRF Token.  Import related operations use http POST and are not possible without a valid CSRF token."
            errorMsg = "%s  Likely reason is that sacapi could not connect to the /api/v1/dataimport/models endpoint, during initial connection." % errorMsg
            raise MissingCSRFTokenError(errorMsg)


    def pushToStaging(self, jobID, tupleList):
        if self.csrfTokenStatus:
            try:
                if type(tupleList) is not list:
                    errorMsg = "Connection upload() method must be called with a list of dicts as tupleList parameter.  Instead it is of type %s" %(type(tupleList))
                    raise ValueError(errorMsg)
                    sampleTuple =  sampleTuple[0]
                    if  type(sampleTuple) is not dict:
                        errorMsg = "Connection upload() method must be called with a list of dicts as tupleList parameter.  Instead a dict, the first element of the list is is of type %s" % (type(sampleTuple))
                        raise ValueError(errorMsg)

                urlJob  = self.urlImportJobs + "/" + jobID
                tupleListString = json.dumps(tupleList)
                postBody = '{ "Data": %s }' % tupleListString
                jobPushResponse = self.oauth.post(urlJob, headers=self.httpPostHeader, data=postBody)
                responseJson = json.loads(jobPushResponse.text)

                return responseJson

            except ValueError as ve:
                raise ve
            except Exception as e:
                errorMsg = "Unknown error during load job creation."
                if e.status_code:
                    errorMsg = "%s  Status code %s from server.  %s" % (errorMsg, e.status_code, e.error)
                    raise RESTError(errorMsg)
                else:
                    errorMsg = "%s  %s" % (errorMsg, e.error)
                    raise Exception(errorMsg)
        else:
            errorMsg = "Missing CSRF Token.  Import related operations use http POST and are not possible without a valid CSRF token."
            errorMsg = "%s  Likely reason is that sacapi could not connect to the /api/v1/dataimport/models endpoint, during initial connection." % errorMsg
            raise MissingCSRFTokenError(errorMsg)


    def deleteJob(self, jobID):
        try:
            urlJob  = self.urlImportJobs + "/" + jobID
            jobDeleteResponse = self.oauth.delete(urlJob, headers=self.httpPostHeader)
            if jobDeleteResponse.status_code != 204:
                errorMsg = "Failed to delete load job %s.  Status code = %s %s" %(jobID, jobDeleteResponse.status_code, jobDeleteResponse.text)
                raise JobDeleteFailure(errorMsg)
        except ValueError as ve:
            raise ve
        except Exception as e:
            errorMsg = "Unknown error during load job deletion."
            if e.status_code:
                errorMsg = "%s  Status code %s from server.  %s" % (errorMsg, e.status_code, e.error)
                raise RESTError(errorMsg)
            else:
                errorMsg = "%s  %s" % (errorMsg, e.error)
                raise Exception(errorMsg)



    def runJob(self, jobID):
        if self.csrfTokenStatus:
            try:
                urlJob  = self.urlImportJobs + "/" + jobID + "/run"
                jobRunResponse = self.oauth.post(urlJob, headers=self.httpPostHeader)
                responseJson = json.loads(jobRunResponse.text)
                return responseJson
            except ValueError as ve:
                raise ve
            except Exception as e:
                errorMsg = "Unknown error during load job deletion."
                if e.status_code:
                    errorMsg = "%s  Status code %s from server.  %s" % (errorMsg, e.status_code, e.error)
                    raise RESTError(errorMsg)
                else:
                    errorMsg = "%s  %s" % (errorMsg, e.error)
                    raise Exception(errorMsg)
        else:
            errorMsg = "Missing CSRF Token.  Import related operations use http POST and are not possible without a valid CSRF token."
            errorMsg = "%s  Likely reason is that sacapi could not connect to the /api/v1/dataimport/models endpoint, during initial connection." % errorMsg
            raise MissingCSRFTokenError(errorMsg)



    def validateLoadJob(self, jobID):
        if self.csrfTokenStatus:
            try:
                urlJobValidate= self.urlImportJobs + "/" + jobID + "/validate"
                jobValidationResponse = self.oauth.post(urlJobValidate, headers=self.httpPostHeader)
                responseJsonV = json.loads(jobValidationResponse.text)

                invalidRowsResponse = self.oauth.get(responseJsonV['invalidRowsURL'])
                invalidRowsResponseV = json.loads(invalidRowsResponse.text)

                responseJsonV["failedRows"] = invalidRowsResponseV['failedRows']
                return responseJsonV

            except Exception as e:
                errorMsg = "Unknown error during load job creation."
                if e.status_code:
                    errorMsg = "%s  Status code %s from server.  %s" % (errorMsg, e.status_code, e.error)
                    raise RESTError(errorMsg)
                else:
                    errorMsg = "%s  %s" % (errorMsg, e.error)
                    raise Exception(errorMsg)
        else:
            errorMsg = "Missing CSRF Token.  Import related operations use http POST and are not possible without a valid CSRF token."
            errorMsg = "%s  Likely reason is that sacapi could not connect to the /api/v1/dataimport/models endpoint, during initial connection." % errorMsg
            raise MissingCSRFTokenError(errorMsg)



    def upload(self, modelMetadata, tupleList, factOnly = True, forceCommit = False, importMethod = "Update"):
        try:
            #First test the mapping.  No point un uploading any data if they'll be rejected on the basis of unmatched columns
            loadResults = {'status': "STARTED", 'responseMessage': ""}

            unmatched = modelMetadata.validateMapping(tupleList)
            if (len(unmatched['unmatchedModelColumns']) > 0) or (len(unmatched['unmatchedImportCols']) > 0):
                errorMsg = "First data tuple has unmatched columns."
                if len(unmatched['unmatchedModelColumns']) > 0:
                    errorMsg = "%s  The following columns are in the SAC model, but not in the tuple: %s." %(errorMsg, unmatched['unmatchedModelColumns'])
                if len(unmatched['unmatchedImportCols']) > 0:
                    errorMsg = "%s  The following columns are in the SAC model, but not in the tuple: %s." %(errorMsg, unmatched['unmatchedImportCols'])
                raise UnmatchedColumnsError(errorMsg)
            else:
                jobID = self.openLoadJob(modelMetadata, factOnly, importMethod)
                pushResponse = self.pushToStaging(jobID, tupleList)

                if (len(pushResponse['failedRows']) > 0) and (forceCommit is False):
                    errorMsg = "Upload Failed!  %s rows failed on initial load" %(len(pushResponse['failedRows']))
                    loadResults['status'] = "FAILED_INITIAL_LOAD"
                    loadResults['responseMessage'] = errorMsg
                    loadResults['failedNumberRows'] = pushResponse['failedRows']
                    self.deleteJob(jobID)
                    raise InvalidRowsError(loadResults)
                else:
                    validationSattus = self.validateLoadJob(jobID)
                    if (validationSattus['failedNumberRows'] > 0 ) and (forceCommit is False):
                        errorMsg = "Upload Failed!  %s rows failed in validation" % (validationSattus['failedNumberRows'])
                        loadResults['status'] = "FAILED_VALIDATION"
                        loadResults['responseMessage'] = errorMsg
                        loadResults['failedNumberRows'] = validationSattus['failedNumberRows']
                        loadResults['failedRows'] = validationSattus['failedRows']
                        self.deleteJob(jobID)
                        raise InvalidRowsError(loadResults)
                    else:
                        commitResponse = self.runJob(jobID)
        except UnmatchedColumnsError as e:
            raise e
        except InvalidRowsError as e:
            raise e
        except JobDeleteFailure as e:
            raise e
        except Exception as e:
            errorMsg = "Unknown error during load job creation."
            if e.status_code:
                errorMsg = "%s  Status code %s from server.  %s" % (errorMsg, e.status_code, e.error)
                raise RESTError(errorMsg)
            else:
                errorMsg = "%s  %s" % (errorMsg, e.error)
                raise Exception(errorMsg)



    def searchProviders(self, searchstr):
        #Use this method to look up a provider ID, if you know the name of the model
        hits = {}
        for provName in self.providerLookup.keys():
            if provName.find(searchstr) > -1:
                hits[provName] = self.providerLookup[provName]
        return hits


    def connect(self, clientID, clientSecret):
        #Wrapper to cut down on the number of commands needed to initiate a session
        try:
            self.getAccessToken(clientID, clientSecret)
            self.getProviders()
        except OAuthError as oa:
            raise oa
        except RESTError as re:
            raise re
        except Exception as e:
            raise e


    def addFilterProvider(self, providerID):
        self.paramManualOverride[providerID] = None
        self.filterOrderBy[providerID] = {}
        self.filterLogicGates[providerID] = self.logicGateOperators.LG_AND
        self.filters[providerID] = []

    def addStringFilter(self, providerID, columnName, filterValue, operator):
        dateDimensions = list(self.modelMetadata[providerID].dateDimensions.keys())
        dimensions = list(self.modelMetadata[providerID].dimensions.keys())
        accounts = list(self.modelMetadata[providerID].accounts.keys())
        if (operator != self.filterStringOperations.CONTAINS) and (operator != self.filterStringOperations.ENDS_WITH) and (operator != self.filterStringOperations.STARTS_WITH):
            errMessage = "Invalid value '%s' passed to string filter operator.  Operator must be one of 'contains', 'startswith', or 'endswith'" %operator
            raise RESTParamsError(errMessage)
        elif (columnName not in dateDimensions) and (columnName not in dimensions) and (columnName not in accounts) and (columnName not in self.modelMetadata[providerID].measures):
            validCols = []
            validCols.extend(dateDimensions)
            validCols.extend(dimensions)
            validCols.extend(accounts)
            validCols.extend(self.modelMetadata[providerID].measures)
            errMessage = "Invalid value '%s' passed as dimension, mrasure or account column selection.  Valid values for this model are %s" % (columnName, validCols)
            raise RESTParamsError(errMessage)
        else:
            filterSubstring = "%s(%s,'%s')" %(operator, columnName, filterValue)
            if (len(self.filters[providerID]) < 1) and (self.filterLogicGates[providerID] == self.LG_NOT):
                filterSubstring = "%s %s" %(self.filterLogicGates[providerID], filterSubstring)
            elif (len(self.filters[providerID]) > 0) and (self.filterLogicGates[providerID] == self.LG_NOT):
                filterSubstring = " and (%s %s)" %(self.filterLogicGates[providerID], filterSubstring)
            elif (len(self.filters[providerID]) > 0):
                filterSubstring = " %s %s" %(self.filterLogicGates[providerID], filterSubstring)
            self.filters[providerID].append(filterSubstring)


    def setFilterOrderBy(self, providerID, orderByCol, ascDesc):
        if (ascDesc != "asc") and (ascDesc != "desc"):
            errMessage = "Invalid value '%s' passed to orderby operator.  Operator must be one of 'asc', or 'desc'" %ascDesc
            raise RESTParamsError(errMessage)
        else:
            self.filterOrderBy[providerID][orderByCol] = ascDesc

    def addLogicalFilter(self, providerID, columnName, filterValue, operator):
        dateDimensions = list(self.modelMetadata[providerID].dateDimensions.keys())
        dimensions = list(self.modelMetadata[providerID].dimensions.keys())
        accounts = list(self.modelMetadata[providerID].accounts.keys())
        if (operator != self.filterOperators.EQUAL) and (operator != self.filterOperators.NOT_EQUAL) and (operator != self.filterOperators.LESS_THAN) and (operator != self.filterOperators.LESS_THAN_OR_EQUAL) and (operator != self.filterOperators.GREATER_THAN) and (operator != self.filterOperators.GREATER_THAN_OR_EQUAL) and (operator != self.filterOperators.NOT_EQUAL):
            errMessage = "Invalid value '%s' passed to logical filter operator.  Operator must be one of 'eq', 'ne', 'gt', 'lt', 'ge', or 'le'" %operator
            raise RESTParamsError(errMessage)
        elif (columnName not in dateDimensions) and (columnName not in dimensions) and (columnName not in accounts) and (columnName not in self.modelMetadata[providerID].measures):
            validCols = []
            validCols.extend(dateDimensions)
            validCols.extend(dimensions)
            validCols.extend(accounts)
            validCols.extend(self.modelMetadata[providerID].measures)
            errMessage = "Invalid value '%s' passed as dimension, mrasure or account column selection.  Valid values for this model are %s" %(columnName, validCols)
            raise RESTParamsError(errMessage)
        else:
            filterSubstring = "%s %s '%s'" %(columnName, operator, filterValue)
            if (len(self.filters[providerID]) < 1) and (self.filterLogicGates[providerID] == self.LG_NOT):
                filterSubstring = "%s %s" %(self.filterLogicGates[providerID], filterSubstring)
            elif (len(self.filters[providerID]) > 0) and (self.filterLogicGates[providerID] == self.LG_NOT):
                filterSubstring = " and (%s %s)" %(self.filterLogicGates[providerID], filterSubstring)
            elif (len(self.filters[providerID]) > 0):
                filterSubstring = " %s %s" %(self.filterLogicGates[providerID], filterSubstring)
            self.filters[providerID].append(filterSubstring)


    def setParamOverride(self, providerID, moValue):
        self.paramManualOverride[providerID] = moValue

    def clearParamOverride(self, providerID):
        self.paramManualOverride[providerID] = None

    def resolveFilter(self, providerID, pagesize = None):
        returnVal = "?"
        ampersand = ""
        if self.paramManualOverride[providerID] is not None:
            returnVal = "%s%s" %(returnVal, self.paramManualOverride[providerID])
            return returnVal
        else:
            if self.filterOrderBy[providerID]:
                providerOrderByCol = list(self.filterOrderBy[providerID].keys())[0]
                providerOrderDir = self.filterOrderBy[providerID][providerOrderByCol]
                returnVal = "%s%s$orderby=%s %s" % (returnVal, ampersand, providerOrderByCol, providerOrderDir)
                ampersand = "&"
            if len(self.filters[providerID]) > 0:
                returnVal = "%s%s$filter=" %(returnVal, ampersand)
                for currentFilter in self.filters[providerID]:
                    returnVal = "%s%s" %(returnVal, currentFilter)
                ampersand = "&"
            if pagesize is not None:
                returnVal = "%s%spagesize=%s" %(returnVal, ampersand, pagesize)
            return returnVal


    def getModelMetadata(self, providerID):
        try:
            modelMetadata = ModelMetadata(providerID)
            urlMetadata = self.urlExportProviderRoot + "/" + providerID + "/$metadata"
            response = self.oauth.get(urlMetadata)

            xmlData = minidom.parseString(response.text.encode("UTF-8"))
            for entityTypeElement in xmlData.getElementsByTagName("EntityType"):
                nameAttribute = entityTypeElement.getAttribute("Name")
                if nameAttribute.find("FactData") > -1:
                    # There will be more than one EntityType element, but only one named "FactData"

                    # all non-measure columns appear in the PropertyRef elements
                    dimList = []
                    for propertyRefElement in entityTypeElement.getElementsByTagName("PropertyRef"):
                        prnAtt = propertyRefElement.getAttribute("Name")
                        dimList.append(prnAtt)

                    # Property elements include all columnsPackb0xcanyon!!
                    for propertyElement in entityTypeElement.getElementsByTagName("Property"):
                        prAtt = propertyElement.getAttribute("Name")
                        dataType = ""

                        # occurs oncce, so this little for loop will fetch us our only String grandchild of Property
                        for stringElement in propertyElement.getElementsByTagName("String"):
                            dataType = stringElement.childNodes[0].data

                        urlCurrDimMetadata = self.urlExportProviderRoot + "/" + providerID + "/" + prAtt + "Master"
                        currDimResponse = self.oauth.get(urlCurrDimMetadata)
                        currDimResponseJson = json.loads(currDimResponse.text)

                        # sort dimensions into the dimension dicts and measures into the measure list

                        if prAtt not in dimList:
                            # Measures and versions show up in the modelMetadata.measures list
                            if prAtt not in modelMetadata.dimensions:
                                modelMetadata.measures.append(prAtt)
                        else:
                            # modelMetadata.dateDimensions
                            # modelMetadata.accounts
                            # modelMetadata.versions
                            # modelMetadata.dimensions
                            isAccount = False
                            isVersion = False
                            isDate = False
                            mdMembers = {}
                            for cdMember in currDimResponseJson["value"]:
                                if "DATE" in cdMember:
                                    isDate = True
                                    cmID = cdMember["DATE"]
                                    mdMembers[cmID] = cmID
                                elif 'VERSION' in cdMember:
                                    isVersion = True
                                    cmID = cdMember["ID"]
                                    cmDesc = cdMember["Description"]
                                    mdMembers[cmID] = cmDesc
                                elif "accType" in cdMember:
                                    isAccount = True
                                    cmID = cdMember["ID"]
                                    cmDesc = cdMember["Description"]
                                    mdMembers[cmID] = cmDesc
                                else:
                                    cmID = cdMember["ID"]
                                    cmDesc = cdMember["Description"]
                                    mdMembers[cmID] = cmDesc
                            if isAccount:
                                modelMetadata.accounts[prAtt] = mdMembers
                            elif isVersion:
                                modelMetadata.versions[prAtt] = mdMembers
                            elif isVersion:
                                modelMetadata.dateDimensions[prAtt] = mdMembers
                            else:
                                modelMetadata.dimensions[prAtt] = mdMembers

            self.modelMetadata[providerID] = modelMetadata
            dimList = list(modelMetadata.dimensions.keys())
            self.addFilterProvider(providerID)
            modelMetadata.initializeMapping()
            return modelMetadata
        except Exception as e:
            errorMsg = "Unknown error during token acquisition."
            if e.status_code:
                errorMsg = "%s  Status code %s from server.  %s" % (errorMsg, e.status_code, e.error)
                raise RESTError(errorMsg)
            else:
                errorMsg = "%s  %s" % (errorMsg, e.error)
                raise Exception(errorMsg)


    def getAuditData(self, modelMetadata):
        try:
            providerID = modelMetadata.modelID
            urlAuditData = self.urlExportProviderRoot + "/" + providerID + "/AuditData"
            response = self.oauth.get(urlAuditData)
            responseJson = json.loads(response.text)
            return responseJson["value"]
        except Exception as e:
            errorMsg = "Unknown error during token acquisition."
            if e.status_code:
                errorMsg = "%s  Status code %s from server.  %s" %(errorMsg, e.status_code, e.error)
                raise RESTError(errorMsg)
            else:
                errorMsg = "%s  %s" %(errorMsg, e.error)
                raise Exception(errorMsg)



    def getFactData(self, modelMetadata, pagesize = None):
        try:
            providerID = modelMetadata.modelID
            filterString = self.resolveFilter(providerID, pagesize)
            urlFactData = self.urlExportProviderRoot + "/" + providerID + "/FactData" + filterString
            fdRecordList = self.factDataRecordRollup(urlFactData)
            return fdRecordList
        except Exception as e:
            errorMsg = "Unknown error during fact data acquisition."
            if e.status_code:
                errorMsg = "%s  Status code %s from server.  %s" %(errorMsg, e.status_code, e.error)
                raise RESTError(errorMsg)
            else:
                errorMsg = "%s  %s" %(errorMsg, e.error)
                raise Exception(errorMsg)

    def factDataRecordRollup(self, urlFactData):
        response = self.oauth.get(urlFactData)
        responseJson = json.loads(response.text)
        fdRecordList = responseJson["value"]
        if "@odata.nextLink" in responseJson:
            fdRecordSubList = self.factDataRecordRollup(responseJson["@odata.nextLink"])
            fdRecordList.extend(fdRecordSubList)
        return fdRecordList


# SPDX-FileCopyrightText: 2023 SAP SE or an SAP affiliate company <david.stocker@sap.com>
#
# SPDX-License-Identifier: Apache-2.0