DROP TABLE IF EXISTS apt_trade;

CREATE TABLE apt_trade (
    id SERIAL PRIMARY KEY,
    거래금액 INTEGER,
    건축년도 INTEGER,
    년 INTEGER,
    월 INTEGER,
    일 INTEGER,
    법정동 TEXT,
    아파트 TEXT,
    전용면적 NUMERIC(7,2),
    층 INTEGER,
    지번 TEXT,
    지역코드 TEXT
);
