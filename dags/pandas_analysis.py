from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import io

# 1. データ生成（Extract相当）
def generate_data():
    data = {
        'id': [10, 20, 30, 40, 50],
        'value': [100, 200, 150, 300, 250]
    }
    df = pd.DataFrame(data)
    print("Generated Data:\n", df)
    return df.to_json()  # タスク間でのデータ受け渡し（XCom）用にJSON化

# 2. データ分析・集計（Transform相当）
def analyze_data(ti):
    # 前のタスクからデータを取得
    json_data = ti.xcom_pull(task_ids='generate_task')
    df = pd.read_json(io.StringIO(json_data))
    
    # 商品ごとの平均売上を算出
    analysis = df.groupby('product')['sales'].mean().reset_index()
    print("Analysis Result (Average Sales):\n", analysis)
    return analysis.to_json()

# 3. 結果の出力（Load相当）
def save_result(ti):
    json_result = ti.xcom_pull(task_ids='analyze_task')
    analysis = pd.read_json(io.StringIO(json_result))
    # 実際にはここでDB保存やファイル出力を行う
    print("Final Result saved successfully.")

# DAGの定義
with DAG(
    dag_id='pandas_analysis_example',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:

    # PythonOperatorによるタスク定義
    t1 = PythonOperator(
        task_id='generate_task',
        python_callable=generate_data
    )

    t2 = PythonOperator(
        task_id='analyze_task',
        python_callable=analyze_data
    )

    t3 = PythonOperator(
        task_id='save_task',
        python_callable=save_result
    )

    # タスクの順序設定
    t1 >> t2 >> t3
