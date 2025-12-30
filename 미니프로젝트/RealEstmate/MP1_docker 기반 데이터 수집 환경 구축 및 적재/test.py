import pandas as pd
from sqlalchemy import create_engine

# 1. DB 연결 설정 (기존 설정과 동일)
engine = create_engine(
    "postgresql+psycopg2://postgres:postgres@localhost:5432/estate_db"
)

def verify_raw_data():
    try:
        # 2. SQL을 통해 raw_data 테이블의 데이터 읽어오기
        # 전체를 다 가져오면 느릴 수 있으니 상위 10개만 확인
        df = pd.read_sql("SELECT * FROM raw_data LIMIT 10", engine)
        
        # 3. 전체 데이터 개수 확인
        count_df = pd.read_sql("SELECT COUNT(*) FROM raw_data", engine)
        total_count = count_df.iloc[0, 0]

        print("="*50)
        print(f"✅ 데이터 검증 결과")
        print(f"- 적재된 전체 행 개수: {total_count}개")
        print("-"*50)
        
        # 4. 컬럼명과 데이터 타입 확인
        print("- 컬럼 리스트 및 샘플 데이터:")
        if not df.empty:
            print(df.head())
        else:
            print("⚠️ 테이블은 존재하지만 데이터가 없습니다.")
        print("="*50)

    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        print("테이블이 존재하지 않거나 DB 연결에 문제가 있을 수 있습니다.")

if __name__ == "__main__":
    verify_raw_data()