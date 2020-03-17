import pysolr
import os
#Solar url connection and access
SOLAR_CONFIGURATION={
    "URL":os.environ["CONNECTIONSTRINGS:SOLRCONNECTIONSTRING"]
}
solr_product= pysolr.Solr(SOLAR_CONFIGURATION["URL"]+"/product_information/", timeout=10,verify=False)
solr_notification_status=pysolr.Solr(SOLAR_CONFIGURATION["URL"]+'/sap_notification_status/', timeout=10,verify=False)
solr_unstructure_data=pysolr.Solr(SOLAR_CONFIGURATION["URL"]+'/unstructure_processed_data/', timeout=10,verify=False)
solr_document_variant=pysolr.Solr(SOLAR_CONFIGURATION["URL"]+'/sap_document_variant/', timeout=10,verify=False)
# Internationalization