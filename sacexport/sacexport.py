import json
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from xml.dom import minidom

class RESTError(ValueError):
    pass

class OAuthError(ValueError):
    pass

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

    def getModelMetadata(self, providerID):
            try:
                modelMetadata = ModelMetadata()
                urlMetadata = self.urlProviderRoot + "/" + providerID + "/$metadata"
                response = self.oauth.get(urlMetadata)

                xmlData = minidom.parseString(response.text.encode("UTF-8"))
                for entityTypeElement in xmlData.getElementsByTagName("EntityType"):
                    nameAttribute = entityTypeElement.getAttribute("Name")
                    if nameAttribute.find("FactData") > -1:
                        #There will be more than one EntityType element, but only one named "FactData"

                        for propertyRefElement in entityTypeElement.getElementsByTagName("PropertyRef"):
                            prnAtt = propertyRefElement.getAttribute("Name")

                            urlCurrDimMetadata = self.urlProviderRoot + "/" + providerID + "/" + prnAtt + "Master"
                            currDimResponse = self.oauth.get(urlCurrDimMetadata)
                            currDimResponseJson = json.loads(currDimResponse.text)

                            if prnAtt.find("Account_") == 0:
                                aMembers = {}
                                for aMember in currDimResponseJson["value"]:
                                    aID = aMember["ID"]
                                    aDesc = aMember["Description"]
                                    if aDesc:
                                        aMembers[aID] = aDesc
                                    modelMetadata.accounts[prnAtt] = aMembers
                            elif prnAtt.find("Version") > -1:
                                modelMetadata.versions[prnAtt] = currDimResponseJson["value"]
                            elif prnAtt.find("Date") > -1:
                                modelMetadata.dateDimensions[prnAtt] = currDimResponseJson["value"]
                            else:
                                mdMembers = {}
                                for cdMember in currDimResponseJson["value"]:
                                    cmID = cdMember["ID"]
                                    cmDesc = cdMember["Description"]
                                    mdMembers[cmID] = cmDesc
                                modelMetadata.dimensions[prnAtt] = mdMembers

                        #Measure columns
                        for propertyElement in entityTypeElement.getElementsByTagName("Property"):
                            pnAtt = propertyElement.getAttribute("Name")
                            if (pnAtt.find("Version") < 0) and (pnAtt not in modelMetadata.dateDimensions.keys()):
                                if pnAtt not in modelMetadata.dimensions:
                                    modelMetadata.measures.append(pnAtt)
                self.modelMetadata[providerID] = modelMetadata
                return modelMetadata
            except Exception as e:
                errorMsg = "Unknown error during token acquisition."
                if e.status_code:
                    errorMsg = "%s  Status code %s from server.  %s" %(errorMsg, e.status_code, e.error)
                    raise RESTError(errorMsg)
                else:
                    errorMsg = "%s  %s" %(errorMsg, e.error)
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



    def getFactData(self, providerID):
        try:
            urlFactData = self.urlProviderRoot + "/" + providerID + "/FactData"
            response = self.oauth.get(urlFactData)
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