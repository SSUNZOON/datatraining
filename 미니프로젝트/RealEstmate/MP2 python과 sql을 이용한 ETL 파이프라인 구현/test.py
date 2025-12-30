import pandas as pd
from sqlalchemy import create_engine

# DB 연결 설정
engine = create_engine(
    "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/estate_db"
)

def verify_with_python():
    print("=== [Python 데이터 검증 시작] ===")
    
    # 1. 전체 데이터 불러오기 (분석용 테이블)
    df = pd.read_sql("SELECT * FROM analytics.apartment_transactions", engine)
    
    # 2. 데이터 타입(Dtype) 및 결측치 확인
    print("\n[1. 데이터 정보 확인]")
    print(df.info()) 
    # 여기서 '거래금액'이 int64(bigint)인지, '구'와 '동'에 Non-Null 값이 가득 차 있는지 확인합니다.

    # 3. 실제 변환된 값 샘플 확인
    print("\n[2. 주요 컬럼 데이터 샘플 (상위 5개)]")
    # 구, 동, 거래금액 컬럼이 예상대로 채워졌는지 확인
    print(df[['구', '동', '거래금액', '아파트']].head())

    # 4. 통계적 검증 (숫자형이 아니면 에러 발생)
    print("\n[3. 구별 평균 거래금액 (숫자형 변환 검증)]")
    # '거래금액'이 숫자형이기에 바로 mean() 연산이 가능합니다.
    summary = df.groupby('구')['거래금액'].mean().sort_values(ascending=False)
    print(summary)

    print("\n✅ 모든 검증이 완료되었습니다.")

if __name__ == "__main__":
    verify_with_python()