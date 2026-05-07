import pandas as pd
import numpy as np
import os
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

def calc_histogram():
    # 1. 大規模データの生成（または読み込み）
    raw_data = np.random.normal(loc=50, scale=15, size=1000000)
    
    # 2. 事前集計（ビン分け）を行う
    counts, bin_edges = np.histogram(raw_data, bins=50)
    
    # 3. 集計結果のみを保持
    hist_df = pd.DataFrame({
        'bin_center': (bin_edges[:-1] + bin_edges[1:]) / 2,
        'count': counts
    })
    
    # 4. Parquet で保存
    save_path = os.path.expanduser("~/airflow/hist_summary.parquet")
    hist_df.to_parquet(save_path, index=False)
    print(f"Summary saved to {save_path}")

with DAG(
    dag_id='big_data_hist_dag_v3',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    calc_task = PythonOperator(
        task_id='calc_hist_task',
        python_callable=calc_histogram
    )



