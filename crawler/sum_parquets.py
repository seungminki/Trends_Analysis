import pandas as pd
import re
from datetime import datetime
# for i in range(1, 4):
#     pd_num = pd.read_parquet(f'parquet/test{i}.parquet')
#     print(pd_num)

'''
parquet 파일 불러오기
pd = pd.read_parquet('parquet/test1.parquet')
print(pd)

2. parquet 구조 확인하기
    2-1. 테이블 형식으로 보기
        parquet-tools show [파일명.parquet]
        ex) parquet-tools show test1.parquet
    
    2-2. 메타데이터 확인
        parquet-tools inspect [파일명.parquet]
    

s3로 가능한
parquet-tools show s3://bucket-name/prefix/*
'''

pd_num_1 = pd.read_parquet('crawler/parquet/test1.parquet')
pd_num_2 = pd.read_parquet('crawler/parquet/test2.parquet')
pd_num_3 = pd.read_parquet('crawler/parquet/test3.parquet')

pd_num_1['topic']='초1'
pd_num_2['topic']='초2'
pd_num_3['topic']='초3'

result = pd.concat([pd_num_1, pd_num_2, pd_num_3], ignore_index=True)

result.click = result.click.apply(lambda x: int(re.sub(r'[^0-9]', '', x)))
result.date = result.date.apply(lambda x: datetime.strptime(x, '%Y.%m.%d. %H:%M'))
result.to_parquet(f"crawler/parquet/test_final.parquet", engine="pyarrow")
print(result)

'''
가상환경 주피터 실행
conda prompt open
conda env list
conda activate <가상환경 명>
pip install jupyter notebook
python -m ipykernel install --user --name <가상환경이름>



'''