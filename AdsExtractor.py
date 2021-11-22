import datetime as dt
from google.ads.googleads.client import GoogleAdsClient
import os
import proto

def extract(query, customer_id) -> dict:
   
    client = GoogleAdsClient.load_from_env()
    
    service = client.get_service("GoogleAdsService")

    search_request = client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = customer_id
    search_request.query = query

    response = service.search_stream(search_request)

    data = []

    for batch in response:
        for row in batch.results:
            data.append(proto.Message.to_dict(row))
    
    login_customer_id = os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"]

    return {
        "azf_extraction_datetime": str(dt.datetime.now()),
        "ads_customer_id": customer_id,
        "ads_login_customer_id": login_customer_id,
        "ads_data": data
    }