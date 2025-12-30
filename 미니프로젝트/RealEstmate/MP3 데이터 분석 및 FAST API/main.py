from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
import pandas as pd
import os

app = FastAPI()

# 환경 변수에서 DB URL 로드
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
@app.get("/api/analysis/price-data")
def get_price_analysis_data():
    try:
        # 확인된 한글 컬럼명을 사용하고, 영문으로 별칭을 붙입니다.
        query = """
        SELECT 
            "전용면적" AS exclusive_area, 
            "건축년도" AS build_year, 
            "층" AS floor, 
            "거래금액" AS deposit 
        FROM apt_trade 
        WHERE "거래금액" IS NOT NULL;
        """
        
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
            
        if df.empty:
            return {"message": "데이터가 없습니다."}
            
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB 조회 오류: {str(e)}")

@app.get("/api/check-columns")
def check_columns():
    # 테이블의 모든 컬럼명을 확인하는 쿼리
    query = "SELECT * FROM apt_trade LIMIT 1;"
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    return {"columns": df.columns.tolist()} 

@app.get("/api/stats/busan")
def get_busan_stats():
    try:
        # 4층~13층 데이터가 정제되어 들어있는 테이블을 쿼리합니다.
        query = """
        SELECT 
            "구", 
            ROUND(AVG("거래금액"), 0) AS avg_price,
            COUNT(*) AS transaction_count
        FROM analytics.apartment_transactions
        GROUP BY "구"
        ORDER BY avg_price DESC;
        """
        
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
            
        if df.empty:
            return {"message": "데이터가 없습니다. 먼저 적재 프로세스를 실행하세요."}
            
        return df.to_dict(orient="records")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB 조회 오류: {str(e)}")

        