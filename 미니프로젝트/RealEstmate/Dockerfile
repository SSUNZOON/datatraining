FROM python:3.10-slim
WORKDIR /app

RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

# 최상위 폴더에 있는 파일을 복사
COPY requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 중요: uvicorn이 '폴더명.파일명:앱이름'을 인식할 수 있도록 경로 수정
# 폴더명에 공백이 있으므로 따옴표를 정확히 사용해야 합니다.
CMD ["uvicorn", "MP3 데이터 분석 및 FAST API.main:app", "--host", "0.0.0.0", "--port", "8000"]