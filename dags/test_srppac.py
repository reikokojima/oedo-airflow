from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import json
import os
import io
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode
import sys
sys.path.append("/home/sharaq/spark-oedo")
from sparkOEDOModules.procModules import constants 
from sparkOEDOModules.detectorProcs.srppac.srppacMain import Process
from sparkOEDOModules.detectorProcs.srppac.srppacMain import Show_2D_Histogram
from sparkOEDOModules.detectorProcs.srppac.srppacMain import Show_1D_Histogram
from sparkOEDOModules.detectorProcs.srppac.srppacMain import filter_sql


jar_path = "/home/sharaq/spark-oedo/scala_package/target/scala-2.13/spark-oedo-package_2.13-1.0.jar"

myspark = (
    SparkSession.builder
    .master("local[10]")
    .config("spark.driver.memory", "12g")      # 必要に応じて増やす
    .config("spark.executor.memory", "12g")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.driver.extraClassPath", jar_path)
    .config("spark.executor.extraClassPath", jar_path)
    .appName("JSONtoDF")
    .getOrCreate()
)

#################################################################
def run_hitsize(**kwargs):
    outfile = kwargs["out_filename"]
    run_dir = kwargs["output_path"]
    config = {
        "sr91x": {"colName": "size","range": (0, 25),"nbins": 26},
        "sr92x": {"colName": "size","range": (0, 25),"nbins": 26},
        "src1x": {"colName": "size","range": (0, 25),"nbins": 26},
        "src2x": {"colName": "size","range": (0, 25),"nbins": 26},
        "sr11x": {"colName": "size","range": (0, 25),"nbins": 26},
        "sr12x": {"colName": "size","range": (0, 25),"nbins": 26},
        "sr91y": {"colName": "size","range": (0, 25),"nbins": 26},
        "sr92y": {"colName": "size","range": (0, 25),"nbins": 26},
        "src1y": {"colName": "size","range": (0, 25),"nbins": 26},
        "src2y": {"colName": "size","range": (0, 25),"nbins": 26},
        "sr11y": {"colName": "size","range": (0, 25),"nbins": 26},
        "sr12y": {"colName": "size","range": (0, 25),"nbins": 26}          
        }
    # -- Write histogram configureation for Streamlit app
    config_path = os.path.join(run_dir, f"config_hitsize.json")
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    
    # --- Set histogram data path ---
    data_path = os.path.join(run_dir, outfile)

    # --- make 1D histogram ----
    Show_1D_Histogram(
        spark=kwargs["spark"],
        filename=kwargs["filename"],
        detectors=kwargs["detectors"],
        config=config,
        output_path=kwargs["output_path"],
        out_filename=kwargs["out_filename"]
    )
    #  Set the pointer to histogram and configuation files
    pointer = {
        "data_path": data_path,
        "config_path": config_path
    }
    # Write the pointer to file 
    pointer_file = os.path.join(run_dir, "hitsize.json")
    tmp = pointer_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(pointer, f, indent=2)

    os.replace(tmp, pointer_file)
    return pointer

#######################################################################
def run_dqdx(**kwargs):
    outfile = kwargs["out_filename"]
    run_dir = kwargs["output_path"]
    config = {
        "sr91x": {"colName": "q0q1","range": (0, 0.4),"nbins": 401},
        "sr92x": {"colName": "q0q1","range": (0, 0.4),"nbins": 401},
        "src1x": {"colName": "q0q1","range": (0, 0.4),"nbins": 401},
        "src2x": {"colName": "q0q1","range": (0, 0.4),"nbins": 401},
        "sr11x": {"colName": "q0q1","range": (0, 0.4),"nbins": 401},
        "sr12x": {"colName": "q0q1","range": (0, 0.4),"nbins": 401},
        "sr91y": {"colName": "q0q1","range": (0, 0.4),"nbins": 401},
        "sr92y": {"colName": "q0q1","range": (0, 0.4),"nbins": 401},
        "src1y": {"colName": "q0q1","range": (0, 0.4),"nbins": 401},
        "src2y": {"colName": "q0q1","range": (0, 0.4),"nbins": 401},
        "sr11y": {"colName": "q0q1","range": (0, 0.4),"nbins": 401},
        "sr12y": {"colName": "q0q1","range": (0, 0.4),"nbins": 401}          
        }
    # -- Write histogram configureation for Streamlit app
    config_path = os.path.join(run_dir, f"config_dqdx.json")
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    
    # --- Set histogram data path ---
    data_path = os.path.join(run_dir, outfile)

    # --- make 1D histogram ----
    Show_1D_Histogram(
        spark=kwargs["spark"],
        filename=kwargs["filename"],
        detectors=kwargs["detectors"],
        config=config,
        output_path=kwargs["output_path"],
        out_filename=kwargs["out_filename"]
    )
    #  Set the pointer to histogram and configuation files
    pointer = {
        "data_path": data_path,
        "config_path": config_path
    }
    # Write the pointer to file 
    pointer_file = os.path.join(run_dir, "dqdx.json")
    tmp = pointer_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(pointer, f, indent=2)

    os.replace(tmp, pointer_file)
    return pointer


#################################################################
def run_chargedist(**kwargs):
    outfile = kwargs["out_filename"]
    run_dir = kwargs["output_path"]
    config = {
        "sr91x": {"colName": ["charge0", "charge1", "charge2"],"range": (0, 100),"nbins": 200},
        "sr92x": {"colName": ["charge0", "charge1", "charge2"],"range": (0, 100),"nbins": 200},
        "src1x": {"colName": ["charge0", "charge1", "charge2"],"range": (0, 100),"nbins": 200},
        "src2x": {"colName": ["charge0", "charge1", "charge2"],"range": (0, 100),"nbins": 200},
        "sr11x": {"colName": ["charge0", "charge1", "charge2"],"range": (0, 100),"nbins": 200},
        "sr12x": {"colName": ["charge0", "charge1", "charge2"],"range": (0, 100),"nbins": 200},
        "sr91y": {"colName": ["charge0", "charge1", "charge2"],"range": (0, 100),"nbins": 200},
        "sr92y": {"colName": ["charge0", "charge1", "charge2"],"range": (0, 100),"nbins": 200},
        "src1y": {"colName": ["charge0", "charge1", "charge2"],"range": (0, 100),"nbins": 200},
        "src2y": {"colName": ["charge0", "charge1", "charge2"],"range": (0, 100),"nbins": 200},
        "sr11y": {"colName": ["charge0", "charge1", "charge2"],"range": (0, 100),"nbins": 200},
        "sr12y": {"colName": ["charge0", "charge1", "charge2"],"range": (0, 100),"nbins": 200}
        }
    
    # -- Write histogram configureation for Streamlit app
    config_path = os.path.join(run_dir, f"config_chargedist.json")
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)

    Show_1D_Histogram(
        spark=kwargs["spark"],
        filename=kwargs["filename"],
        detectors=kwargs["detectors"],
        config=config,
        output_path=kwargs["output_path"],
        out_filename=kwargs['out_filename']
        )
        
    data_path = os.path.join(run_dir, outfile)

    #  Set the pointer to histogram and configuation files
    pointer = {
        "data_path": data_path,
        "config_path": config_path
    }
    # Write the pointer to file 
    pointer_file = os.path.join(run_dir, "chargedist.json")
    tmp = pointer_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(pointer, f, indent=2)

    os.replace(tmp, pointer_file)
    return pointer

#################################################################

def run_id_vs_charge(**kwargs):
    outfile = kwargs["out_filename"]
    run_dir = kwargs["output_path"]
    config = {
        "sr91x": {"colName": ["id0", "charge0"],"range": [(0, 94), (0, 100)],"nbins": (94, 100)},
        "sr91y": {"colName": ["id0", "charge0"],"range": [(0, 58), (0, 100)],"nbins": (58, 100)},
        "sr92x": {"colName": ["id0", "charge0"],"range": [(0, 94), (0, 100)],"nbins": (94, 100)},
        "sr92y": {"colName": ["id0", "charge0"],"range": [(0, 58), (0, 100)],"nbins": (58, 100)},
        "src1x": {"colName": ["id0", "charge0"],"range": [(0, 94), (0, 100)],"nbins": (94, 100)},
        "src1y": {"colName": ["id0", "charge0"],"range": [(0, 58), (0, 100)],"nbins": (58, 100)},
        "src2x": {"colName": ["id0", "charge0"],"range": [(0, 94), (0, 100)],"nbins": (94, 100)},
        "src2y": {"colName": ["id0", "charge0"],"range": [(0, 58), (0, 100)],"nbins": (58, 100)},
        "sr11x": {"colName": ["id0", "charge0"],"range": [(0, 94), (0, 100)],"nbins": (94, 100)},
        "sr11y": {"colName": ["id0", "charge0"],"range": [(0, 58), (0, 100)],"nbins": (58, 100)}, 
        "sr12x": {"colName": ["id0", "charge0"],"range": [(0, 94), (0, 100)],"nbins": (94, 100)},
        "sr12y": {"colName": ["id0", "charge0"],"range": [(0, 58), (0, 100)],"nbins": (58, 100)}
        }
    # -- Write histogram configureation for Streamlit app
    config_path = os.path.join(run_dir, f"config_id_vs_charge.json")
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)

    # --- Set histogram data path ---
    data_path = os.path.join(run_dir, outfile)
    # --- make 2D histogram ----   
    Show_2D_Histogram(
        spark=kwargs["spark"],
        filename=kwargs["filename"],
        detectors=kwargs["detectors"],
        config=config,
        output_path=kwargs["output_path"],
        out_filename=outfile
    )
    #  Set the pointer to histogram and configuation files
    pointer = {
        "data_path": data_path,
        "config_path": config_path
    }

    pointer_file = os.path.join(run_dir, "id_vs_charge.json")
    # Write the pointer to file 
    tmp = pointer_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(pointer, f, indent=2)

    os.replace(tmp, pointer_file)
    return pointer

#######################################################################

def run_timing_vs_charge(**kwargs):
    outfile = kwargs["out_filename"]
    run_dir = kwargs["output_path"]
    from sparkOEDOModules.detectorProcs.srppac.srppacMain import Show_2D_Histogram
    config = {
        "sr91a": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 300)],"nbins": (400, 100)},
        "sr91x": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 100)],"nbins": (400, 100)},
        "sr91y": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 100)],"nbins": (400, 100)},
        "sr92a": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 300)],"nbins": (400, 100)},
        "sr92x": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 100)],"nbins": (400, 100)},
        "sr92y": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 100)],"nbins": (400, 100)},
        "src1a": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 300)],"nbins": (400, 100)},
        "src1x": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 100)],"nbins": (400, 100)},        
        "src1y": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 100)],"nbins": (400, 100)},   
        "src2a": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 300)],"nbins": (400, 100)},     
        "src2x": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 100)],"nbins": (400, 100)},        
        "src2y": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 100)],"nbins": (400, 100)},  
        "sr11a": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 300)],"nbins": (400, 100)},
        "sr11x": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 100)],"nbins": (400, 100)},
        "sr11y": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 100)],"nbins": (400, 100)},
        "sr12a": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 300)],"nbins": (400, 100)},
        "sr12x": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 100)],"nbins": (400, 100)},
        "sr12y": {"colName": ["timing0", "charge0"],"range": [(-3000,1000),(0, 100)],"nbins": (400, 100)}
        }   
    # -- Write histogram configureation for Streamlit app
    config_path = os.path.join(run_dir, f"config_timing_vs_charge.json")
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    # --- Set histogram data path ---
    data_path = os.path.join(run_dir, outfile)
    # --- make 2D histogram ---- 
    Show_2D_Histogram(
        spark=kwargs["spark"],
        filename=kwargs["filename"],
        detectors=kwargs["detectors"],
        config=config,
        output_path=kwargs["output_path"],
        out_filename=outfile
    )
     #  Set the pointer to histogram and configuation files
    pointer = {
        "data_path": data_path,
        "config_path": config_path
    }
    # Write the pointer to file 
    pointer_file = os.path.join(run_dir, "timing_vs_charge.json")
    tmp = pointer_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(pointer, f, indent=2)
    os.replace(tmp, pointer_file)
    return pointer

#################################### Histogram manifest ##############
def finalize_manifest(**kwargs):
    run_id = kwargs["run_id"]
    run_dir =  kwargs["output_path"] # directory path where the histgram data and configuration files are stored

    manifest = {
        "run_id": run_id,
        "hitsize": {
            "data_path": os.path.join(run_dir, f"hitsize_{run_id}.parquet"),
            "config_path": os.path.join(run_dir, f"config_hitsize.json")
        },
        "chargedist": {
            "data_path": os.path.join(run_dir, f"chargedist_{run_id}.parquet"),
            "config_path": os.path.join(run_dir, f"config_chargedist.json")
        },
        "id_vs_charge": {
            "data_path": os.path.join(run_dir, f"id_vs_charge_{run_id}.parquet"),
            "config_path": os.path.join(run_dir, f"config_id_vs_charge.json")
        },
        "timing_vs_charge": {
            "data_path": os.path.join(run_dir, f"timing_vs_charge_{run_id}.parquet"),
            "config_path": os.path.join(run_dir, f"config_timing_vs_charge.json")
        },
        "dqdx": {
            "data_path": os.path.join(run_dir, f"dqdx_{run_id}.parquet"),
            "config_path": os.path.join(run_dir, f"config_dqdx.json")
        }
    }

    manifest_path = os.path.join(run_dir, "manifest.json") # histogram path to be shown
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    return manifest_path


def update_current_manifest(**kwargs):
    base_dir = kwargs["output_path"]
    run_id   = kwargs["run_id"]
    run_dir  = os.path.join(base_dir, f"run{run_id}")
    manifest_path = os.path.join(run_dir, f"manifest.json")

    pointer = {
        "run_id":run_id,
        "run_dir": run_dir,
        "manifest_path": manifest_path
    }

    pointer_file = os.path.join(base_dir, "current_manifest.json") # setting file to be read from app.py

    # ⭐ atomic write（重要）
    tmp = pointer_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(pointer, f, indent=2)

    os.replace(tmp, pointer_file)

# DAGの定義
with DAG(
    dag_id='srppac_test_dag',
    start_date=datetime(2026, 4, 21),
    catchup=False
) as dag:
    

    run_id = "0094"
    base_dir = "/home/sharaq/sh12s24/hist/srppac"
    run_dir = os.path.join(base_dir, f"run{run_id}")
    os.makedirs(run_dir, exist_ok=True)

    # PythonOperatorによるタスク定義
    t1 = PythonOperator(
        task_id='data_process_task',
        python_callable=Process,
        op_kwargs={
            "spark": myspark,
            "decoder": "V1190",
            "filename": "/home/sharaq/sh12s24/ridf/optics0094_segdata.parquet",
            "tref_file": "/home/sharaq/spark-oedo/map_files/tref.csv",
            "detector_names": ["sr91", "sr92","src1", "src2", "sr11","sr12"],
            "outfile_path": "/home/sharaq/sh12s24/ridf/srppac_processed_0094.parquet"
            },
    )

    t2 = PythonOperator(
        task_id='make_hit_size_hist_task',
        python_callable=run_hitsize,
        op_kwargs={
            "spark": myspark,
            "filename": f"/home/sharaq/sh12s24/ridf/srppac_processed_{run_id}.parquet",
            "detectors": ["sr91x", "sr91y","sr92x","sr92y","src1x","src1y"],
            "output_path": run_dir,
            "out_filename": f"hitsize_{run_id}.parquet"
        }
    )

    t3 = PythonOperator(
        task_id="make_id_charge_hist_task",
        python_callable=run_id_vs_charge,
        op_kwargs={
            "spark": myspark,
            "filename": f"/home/sharaq/sh12s24/ridf/srppac_processed_{run_id}.parquet",
            "detectors": ["sr91x","sr91y","src1x", "src1y"],
            "output_path": run_dir,
            "out_filename": f"id_vs_charge_{run_id}.parquet"
        }
    )
    
    t4 = PythonOperator(
        task_id="make_timing_charge_hist_task",
        python_callable=run_timing_vs_charge,
        op_kwargs={
            "spark": myspark,
            "filename": f"/home/sharaq/sh12s24/ridf/srppac_processed_{run_id}.parquet",
            "detectors": ["sr91a","sr91x","sr91y","sr92x","sr92y","src1x","src1y"],
            "output_path": run_dir,
            "out_filename": f"timing_vs_charge_{run_id}.parquet"
        }
    )
    t5 = PythonOperator(
        task_id="make_histgram_manifest_task",
        python_callable=finalize_manifest,
        op_kwargs={
            "run_id": run_id,
            "output_path": run_dir
        }
    )

    t6 = PythonOperator(
        task_id="update_current_manifest_task",
        python_callable=update_current_manifest,
        op_kwargs={
            "run_id": run_id,
            "output_path": base_dir
        }
    )

    t7 = PythonOperator(
        task_id="make_charge_hist",
        python_callable=run_chargedist,
        op_kwargs={
            "spark": myspark,
            "filename": f"/home/sharaq/sh12s24/ridf/srppac_processed_{run_id}.parquet",
            "detectors": ["sr91x","sr91y","sr92x","sr92y","src1x", "src1y","src2x","src2y"],
            "output_path": run_dir,
            "out_filename": f"chargedist_{run_id}.parquet"
        }
    )

    t8 = PythonOperator(
        task_id="make_dqdx_hist",
        python_callable=run_dqdx,
        op_kwargs={
            "spark": myspark,
            "filename": f"/home/sharaq/sh12s24/ridf/srppac_processed_{run_id}.parquet",
            "detectors": ["sr91x","sr91y"],
            "output_path": run_dir,
            "out_filename": f"dqdx_{run_id}.parquet"
        }
    )
    

    # タスクの順序設定
    #t1 >> 
    t1 >>t5 >> t6 >> [t2,t4,t3,t7,t8] 

