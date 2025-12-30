import pandas as pd
import re
from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/estate_db"
)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    # 1. 컬럼명 정리
    df.columns = df.columns.str.strip()

    # 2. 숫자형 데이터 정제
    # 2-1. '거래금액' (기존)
    if "거래금액" in df.columns:
        df["거래금액"] = df["거래금액"].astype(str).str.replace(",", "", regex=False).str.strip().replace('', '0').astype(int)

    # 2-2. '건축년도' (기존)
    if "건축년도" in df.columns:
        df["건축년도"] = pd.to_numeric(df["건축년도"], errors='coerce').fillna(0).astype(int)

    # 2-3. '전용면적' (기존)
    if "전용면적" in df.columns:
        df["전용면적"] = pd.to_numeric(df["전용면적"], errors='coerce').fillna(0.0).astype(float)

    # 2-4. '층' 추가 (문자열 -> 정수형)
    if "층" in df.columns:
        # pd.to_numeric을 쓰면 '1', '15' 같은 문자는 숫자로 바꾸고, 이상한 값은 NaN으로 처리합니다.
        df["층"] = pd.to_numeric(df["층"], errors='coerce').fillna(0).astype(int)

    # 3. '주소' 컬럼 생성 및 구/동 추출 (기존 로직 유지)
    df['주소'] = "부산광역시 강서구 " + df['법정동'].astype(str) + " " + df['지번'].astype(str)

    def extract_gu_dong(addr):
        if not addr or pd.isna(addr): return (None, None)
        m = re.search(r"(부산광역시\s+\S+구)\s+(\S+동)", addr)
        if m: return (m.group(1), m.group(2))
        return (None, None)

    extracted = [extract_gu_dong(addr) for addr in df['주소']]
    df['구'], df['동'] = zip(*extracted)

    # 4. 필수 데이터 누락 행 제거
    df = df.dropna(subset=["거래금액", "구", "동"])
    
    # 건축년도: 정수형(int)
    df["건축년도"] = pd.to_numeric(df["건축년도"], errors='coerce').fillna(0).astype(int)
    # 전용면적: 실수형(float, 소수점 포함)
    df["전용면적"] = pd.to_numeric(df["전용면적"], errors='coerce').fillna(0.0).astype(float)
    # 층: 정수형(int)
    df["층"] = pd.to_numeric(df["층"], errors='coerce').fillna(0).astype(int)

    # 5. 데이터 필터링 (전용면적이 0보다 크고, 층수가 4층 ~ 13층인 데이터만 추출)
    df = df[
        (df["전용면적"] > 0) & 
        (df["층"] >= 4) & 
        (df["층"] <= 13)
    ]
    
    return df

def verify_table(engine):
    print("\n=== [데이터 적재 결과 확인] ===")
    # 1. 컬럼 정보 출력
    columns_df = pd.read_sql("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'analytics' AND table_name = 'apartment_transactions'
    """, engine)
    print("■ 컬럼 정보:\n", columns_df)

    # 2. 상위 데이터 출력
    data_df = pd.read_sql("SELECT * FROM analytics.apartment_transactions LIMIT 5", engine)
    print("\n■ 데이터 샘플:\n", data_df)

def main():
    try:
        # Step 1: Extract (MP1에서 지정한 테이블명 확인)
        print(">>> [Extract] apt_trade 테이블에서 데이터를 읽어오는 중...")
        df_raw = pd.read_sql("SELECT * FROM apt_trade", engine)

        if df_raw.empty:
            print("❌ 데이터가 없습니다. extract.py를 먼저 실행하세요.")
            return

        # Step 2: Transform
        print(">>> [Transform] 데이터 변환 및 주소 파싱 시작...")
        df_raw['주소'] = "부산광역시 강서구 " + df_raw['법정동'].astype(str) + " " + df_raw['지번'].astype(str)
        print(f"생성된 주소 예시: {df_raw['주소'].iloc[0]}")
        print(f"변환 전 데이터 건수: {len(df_raw)}")

    # 3. 데이터 변환 (여기서 '구', '동'이 추출됩니다)
        df_clean = transform_data(df_raw)

        # Step 3: Load
        print(f">>> [Load] {len(df_clean)}건의 데이터를 analytics 스키마에 적재 중...")
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
            
        df_clean.to_sql(
            name="apartment_transactions",
            schema="analytics",
            con=engine,
            if_exists="replace",
            index=False
        )
        print("✅ 성공: analytics.apartment_transactions 적재 완료")
        verify_table(engine)

    except Exception as e:
        print(f"❌ 오류 발생: {e}")


if __name__ == "__main__":
    main()