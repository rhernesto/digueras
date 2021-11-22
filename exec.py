from analytics_extractor import GoogleAnalytics3
from analytics_extractor import GoogleAnalytics4
from S3 import S3
import AdsExtractor
from facebookAdsExtractor import FacebookAdsExtractor


def main(request, ctx):
    
    if(request['Choice'] == 'GA3'): 
        try:
            ga = GoogleAnalytics3(request['token'])
            result = ga.get_data(request['metricFields'], request['metricInsights'], request['ViewID'], request['startDate'], request['endDate'])

        except Exception as e:
            raise Exception(f"Falha ao executar chamada da API, verifique os parametros ERROR:{e}")
            
    elif(request['Choice'] == 'GA4'):
        try:
            ga = GoogleAnalytics4()
            result = ga.run_report(request['ViewID'],request['metricFields'], request['metricInsights'], request['startDate'], request['endDate'])

        except Exception as e:
            raise Exception(f"Falha ao executar chamada da API, verifique os parametros ERROR:{e}")
            

    elif(request['Choice'] == 'G_ADS'):
        try:
             result = AdsExtractor.extract(request['query'], request['ViewID'])
        except Exception as e:
            raise Exception(f"Falha ao executar chamada da API, verifique os parametros ERROR:{e}")
          

    elif(request['Choice'] == 'FB_ADS'):
        try:
            fb = FacebookAdsExtractor(request['ID'], request['token']) 
            result = fb.get_ad_data(request['ID'], 
                                request['metricFields'], 
                                since=request['startDate'], 
                                until=request['endDate'],
                                breakdowns=request['breakdowns'],
                                level=request['level'],
                                time_increment=request['time_increment'],
                                filtering=request['filtering'])
        except Exception as e:
            raise Exception(f"Falha ao executar chamada da API, verifique os parametros ERROR:{e}")
            
            
          
    else:
        raise Exception("Escolha de API incorreta")
       
        
    # Upload no AWS
    try:
        aws = S3()
        aws.upload(request['bucket'], result, request['fileName'])
    except Exception as e:
        raise Exception(f"Falha ao executar o upload, favor verifique se o bucket está correto ERROR:{e}")
            

# aws.download_s3_file("testebuckettiagobrivia", "bronze/test.json", "downloaded.json")
# Criação do .json
# with open("data.json", "w") as f:
#     json.dump(result,f,indent = 2)