import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import requests
import numpy as np  # 중앙값 계산을 위해 추가

# 1. API 데이터 호출
try:
    response = requests.get("http://localhost:8000/api/analysis/price-data")
    data = response.json()
    df = pd.DataFrame(data)
    
    # 데이터 전처리
    df['deposit'] = df['deposit'].str.replace(',', '').astype(float)
    df['exclusive_area'] = pd.to_numeric(df['exclusive_area'])
    df['build_year'] = pd.to_numeric(df['build_year'])
    df['floor'] = pd.to_numeric(df['floor'])
    
    # 평당 가격 계산
    df['pyeong'] = df['exclusive_area'] / 3.3 
    df['price_per_pyeong'] = df['deposit'] / df['pyeong'] 
    
    print("중앙값 분석 데이터 준비 완료!")
except Exception as e:
    print(f"오류 발생: {e}")
    exit()

# 한글 설정
plt.rc('font', family='Malgun Gothic')
plt.rcParams['axes.unicode_minus'] = False

# 2. 1행 3열 그래프 구성
fig, axes = plt.subplots(1, 3, figsize=(22, 7))

# (1) 전용면적 vs 전체 가격 (상관관계)
sns.regplot(ax=axes[0], data=df, x='exclusive_area', y='deposit', 
            scatter_kws={'alpha':0.4}, line_kws={'color':'red'})
axes[0].set_title('전용면적별 가격 상관관계')
axes[0].set_ylabel('보증금 (만원)')

# (2) [수정] 건축연도별 '평당 가격 중앙값'
# estimator=np.median 을 추가하여 평균 대신 중앙값을 그립니다.
sns.barplot(ax=axes[1], data=df, x='build_year', y='price_per_pyeong', 
            palette='viridis', estimator=np.median)
axes[1].set_title('건축연도별 평당 가격 중앙값')
axes[1].set_ylabel('평당 가격 중앙값 (만원)')
plt.sca(axes[1])
plt.xticks(rotation=45)

# (3) [수정] 층수별 '평당 가격 중앙값' 추이
# 중앙값을 계산한 그룹 데이터를 선그래프로 시각화합니다.
median_by_floor = df.groupby('floor')['price_per_pyeong'].median().reset_index()
sns.lineplot(ax=axes[2], data=median_by_floor, x='floor', y='price_per_pyeong', 
             marker='o', color='green')
axes[2].set_title('층수별 평당 가격 중앙값 추이')
axes[2].set_ylabel('평당 가격 중앙값 (만원)')

plt.tight_layout()
plt.show()