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



class SACConnection(object):
    def __init__(self, tenantName, dataCenter):
        self.tenantName = tenantName
        self.dataCenter = dataCenter
        self.connectionNamespace = "sap"
        self.urlAccessToken = "https://" + tenantName + ".authentication." + dataCenter + ".hana.ondemand.com/oauth/token"
        self.urlNamespaces = "https://" + tenantName + "." + dataCenter + ".sapanalytics.cloud/api/v1/dataexport/administration/Namespaces"
        self.urlProviders = "https://" + tenantName + "." + dataCenter + ".sapanalytics.cloud/api/v1/dataexport/administration/Namespaces(NamespaceID='sac')/Providers"
        self.urlProviderRoot = "https://" + tenantName + "." + dataCenter + ".sapanalytics.cloud/api/v1/dataexport/providers/sac"
        self.accessToken = None
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
            response = self.oauth.get(self.urlProviders)
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
            errorMsg = "Unknown error during token acquisition."
            if e.status_code:
                errorMsg = "%s  Status code %s from server.  %s" %(errorMsg, e.status_code, e.error)
                raise RESTError(errorMsg)
            else:
                errorMsg = "%s  %s" %(errorMsg, e.error)
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
            modelMetadata = ModelMetadata()
            urlMetadata = self.urlProviderRoot + "/" + providerID + "/$metadata"
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

                    # Property elements include all columns
                    for propertyElement in entityTypeElement.getElementsByTagName("Property"):
                        prAtt = propertyElement.getAttribute("Name")
                        dataType = ""

                        # occurs oncce, so this little for loop will fetch us our only String grandchild of Property
                        for stringElement in propertyElement.getElementsByTagName("String"):
                            dataType = stringElement.childNodes[0].data

                        urlCurrDimMetadata = self.urlProviderRoot + "/" + providerID + "/" + prAtt + "Master"
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
            return modelMetadata
        except Exception as e:
            errorMsg = "Unknown error during token acquisition."
            if e.status_code:
                errorMsg = "%s  Status code %s from server.  %s" % (errorMsg, e.status_code, e.error)
                raise RESTError(errorMsg)
            else:
                errorMsg = "%s  %s" % (errorMsg, e.error)
                raise Exception(errorMsg)


    def getAuditData(self, providerID):
        try:
            urlAuditData = self.urlProviderRoot + "/" + providerID + "/AuditData"
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



    def getFactData(self, providerID, pagesize = None):
        try:
            filterString = self.resolveFilter(providerID, pagesize)
            urlFactData = self.urlProviderRoot + "/" + providerID + "/FactData" + filterString
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


if __name__ == "__main__":
    sac = SACConnection("appdesign", "eu10")
    sac.connect("sb-90703045-f504-4fed-b5b3-1a408e309e85!b16780|client!b3650", "Qlpl07Z7ehH9EDoTLJ21nZofBys=")


    # New Model

    hits = sac.searchProviders("TechEd2021")
    md = sac.getModelMetadata('Cdlvmnd17edjumrnshekknxo8w')
    sac.addStringFilter('Cdlvmnd17edjumrnshekknxo8w', "Region", "Inter", sac.filterStringOperations.STARTS_WITH)
    sac.addLogicalFilter('Cdlvmnd17edjumrnshekknxo8w', "NationalParkUnitType", "National Park", sac.filterOperators.EQUAL)
    sac.addLogicalFilter('Cdlvmnd17edjumrnshekknxo8w', "Date", "197901", sac.filterOperators.EQUAL)
    sac.setFilterOrderBy('Cdlvmnd17edjumrnshekknxo8w', "State", "asc")
    sac.setParamOverride('Cdlvmnd17edjumrnshekknxo8w', "$top=5&$orderby=State asc&$filter=startswith(Region,'Inter') and State ne 'CO'")
    #sac.setParamOverride('Cdlvmnd17edjumrnshekknxo8w', "$top=10&pagesize=5")
    sac.clearParamOverride('Cdlvmnd17edjumrnshekknxo8w')
    fd = sac.getFactData('Cdlvmnd17edjumrnshekknxo8w', 10)



    """
    # Account Model
    hits = sac.searchProviders("BestRunJuice_SampleModel")
    md = sac.getModelMetadata('sap.epm:BestRunJuice_SampleModel')
    sac.addLogicalFilter('sap.epm:BestRunJuice_SampleModel', "Account_BestRunJ_sold", "Quantity_sold", sac.filterOperators.EQUAL)
    sac.addLogicalFilter('sap.epm:BestRunJuice_SampleModel', "Product_3e315003an", "PD1", sac.filterOperators.EQUAL)
    sac.setFilterOrderBy('sap.epm:BestRunJuice_SampleModel', "Store_3z2g5g06m4", "asc")
    fd = sac.getFactData('sap.epm:BestRunJuice_SampleModel')
    """

    hello = "world"