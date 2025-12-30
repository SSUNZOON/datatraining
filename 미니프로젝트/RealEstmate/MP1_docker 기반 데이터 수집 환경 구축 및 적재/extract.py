
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from sqlalchemy import create_engine

url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
svckey = "2ac23bc68b62e0d687f54a3dc71276b1344cd7436b848534da482478a708600a"


def parse_item(xml_text: str):
    root = ET.fromstring(xml_text)
    items = root.findall(".//item")
    rows = [] 
    
    for item in items:
        d = {}
        for child in list(item):
            d[child.tag] = (child.text or "").strip()
        rows.append(d)
    return rows

def main():    
    params = {
        'serviceKey' : svckey,
        'pageNo' : 1,
        'numOfRows' : 1000,
        'LAWD_CD': '26440',
        'DEAL_YMD': '202401'    
    }
    response = requests.get(url, params=params)
    rows = parse_item(response.text)
    
    df = pd.DataFrame(rows)


    df = df.rename(columns={
        "dealAmount": "거래금액",
        "buildYear": "건축년도",
        "dealYear": "년",
        "dealMonth": "월",
        "dealDay": "일",
        "umdNm": "법정동",
        "aptNm": "아파트",
        "excluUseAr": "전용면적",
        "floor": "층",
        "jibun": "지번",
        "sggCd": "지역코드"
    })

    df = df[
        ["거래금액", "건축년도", "년", "월", "일",
        "법정동", "아파트", "전용면적", "층", "지번", "지역코드"]
    ]

    engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@localhost:5432/estate_db"
    )  
    df.to_sql(
        name="raw_data",
        con=engine,
        if_exists="replace",   # 처음엔 replace, 이후 append 권장
        index=False
    )
    print("PostgreSQL 적재 완료")

main()

    
