# app.py

import streamlit as st
import pandas as pd
import os
import matplotlib.pyplot as plt
from streamlit_autorefresh import st_autorefresh
import json
import glob


BASE_DIR = "/home/sharaq/sh12s24/hist/srppac"
pointer_file = os.path.join(BASE_DIR, "current_manifest.json")



try:
    with open(pointer_file) as f:
        pointer = json.load(f)
except Exception as e:
    st.error(f"manifest取得失敗: {e}")
    st.stop()

RUN_DIR = pointer["run_dir"]

manifest_path = pointer["manifest_path"]
with open(manifest_path) as f:
    manifest = json.load(f)

run_id=pointer["run_id"]

st.title("SRPPAC Viewer")

tab1, tab2,tab3,tab4,tab5 = st.tabs(["1:hit_size", "2:ID vs Charge","3:Timing vs Charge","4:Charge Dist.","5:Q0-Q1/Q0+Q1"])



###### ---- 更新ボタン ----
#if st.button("更新"):
#    st.rerun()

with tab1:
    try:
        df = pd.read_parquet(
            manifest["hitsize"]["data_path"],
            engine="pyarrow")
    except Exception as e:
        st.error(f"読み込み失敗: {e}")
        st.stop()

    # ---- config 読み込み ----
    try:
        with open(manifest["hitsize"]["config_path"]) as f:
            config = json.load(f)
    except Exception as e:
        st.error(f"config読み込み失敗: {e}")
        st.stop()

    valid_hist_names = df["hist_name"].dropna().unique()
    histx = df["colName"].dropna().unique()  # histx=["size"]
    colors = ["tab:blue", "tab:orange", "tab:green"]

    ppac_list = sorted(set(h[:-1] for h in valid_hist_names))   #"sr91","sr92"
    # ---- select detector
    selected_detector = st.selectbox("Detector", ppac_list,key="hitsize")
    # ---- スケール ----
    scale_mode = st.radio(f"Scale",["linear", "log"],horizontal=True,
                    key=f"scale_hitsize")
            
    col1, col2 = st.columns(2)
   
    for col, axis in zip([col1, col2], ["x", "y"]):
        with col:
            detector_plane = f"{selected_detector}{axis}"

            fig, ax = plt.subplots()


            for ch, color in zip(histx, colors):
                d = df[
                    (df["hist_name"] == detector_plane) &
                    (df["colName"] == ch)
                    ]
                
                if d.empty:
                    continue
                # ---- range取得 ----
                if detector_plane not in config:
                    st.warning("detector設定なし")
                    st.stop()

                conf = config[detector_plane]
                x_min, x_max = conf["range"]
             
            # ---- 描画 ----
                ax.bar(
                    d["x_bin"],   # または bin中心
                    d["z"],       # カウント
                    label=ch,
                    color=color,
                    alpha=0.8
                    )
                

            if scale_mode == "log":
                ax.set_yscale("log")
            ax.set_xlim(x_min,x_max)
            ax.set_title("RUN"+run_id+" "+detector_plane)
            ax.set_yscale(scale_mode)
            ax.set_xlabel("hitsize")
            # ---- 凡例 ----
            ax.legend()
            st.pyplot(fig)



with tab2:
    import numpy as np

    # ---- 読み込み ----
    try:
        df = pd.read_parquet(manifest["id_vs_charge"]["data_path"])
    except Exception as e:
        st.error(f"読み込み失敗: {e}")
        st.stop()

    # ---- config（2D）----
    try:
        with open(manifest["id_vs_charge"]["config_path"]) as f:
            config = json.load(f)
    except Exception as e:
        st.error(f"config読み込み失敗: {e}")
        st.stop()
    
    valid_hist_names = df["hist_name"].dropna().unique() 

  # ---- detector選択 ----
    selected_detector = st.selectbox("Detector", valid_hist_names,key="id_vs_charge")
    df_det = df[df["hist_name"] == selected_detector]

    # ---- データ整形 ----
    d_agg = df_det.groupby(["x_bin", "y_bin"])["z"].sum().reset_index()
    pivot = d_agg.pivot(index="y_bin", columns="x_bin", values="z").fillna(0)
 #   cmap = st.selectbox("Colormap",["jet","viridis", "plasma", "inferno", "magma"],
 #   key=f"cmap_{selected_detector}_id_vs_charge")
 #   
    # ---- range取得 ----
    if selected_detector not in config:
        st.warning("detector設定なし")
        st.stop()

    #  -- asign the configration of selected detector to conf

    conf = config[selected_detector]

    try:
        x_min, x_max = conf["range"][0]
        y_min, y_max = conf["range"][1]
    except Exception as e:
        st.error(f"range設定が不正: {e}")
        st.stop()

   # --- 表示scale 選択 ------
    scale_mode = st.radio(
        "Scale",
        ["log","linear"],
        horizontal=True,
        key=f"scale_{selected_detector}_id_vs_charge"
        )

    if scale_mode == "log":
        from matplotlib.colors import LogNorm
        norm = LogNorm()
        z = pivot.values.astype(float)
        z[z <= 0] = np.nan
    else:
        norm = None
        z = pivot.values

    # ---- 描画 ----
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots()
    im = ax.imshow(
        z,
        origin="lower",
        aspect="auto",
        extent=[x_min, x_max, y_min, y_max],
        norm=norm,
        cmap="jet")

    ax.set_title(f"RUN{run_id} {selected_detector} ID0 vs Charge0")
    ax.set_xlabel("ID0")
    ax.set_ylabel("Charge0")
    ax.grid(False)

    fig.colorbar(im, ax=ax)
    st.pyplot(fig)


with tab3:
    import json
    import numpy as np

    # ---- 読み込み ----
    try:
        df = pd.read_parquet(manifest["timing_vs_charge"]["data_path"])
    except Exception as e:
        st.error(f"data file 読み込み失敗: {e}")
        st.stop()

    # ---- config（2D）----
    try:
        with open(manifest["timing_vs_charge"]["config_path"]) as f:
            config = json.load(f)
    except Exception as e:
        st.error(f"config読み込み失敗: {e}")
        st.stop()

    valid_hist_names = df["hist_name"].dropna().unique()

    # ---- detector選択 ----
    selected_detector = st.selectbox("Detector", valid_hist_names, key="timing_vs_charge")   
    df_det = df[df["hist_name"] == selected_detector]

    # ---- データ整形 ----
    d_agg = df_det.groupby(["x_bin", "y_bin"])["z"].sum().reset_index()
    pivot = d_agg.pivot(index="y_bin", columns="x_bin", values="z").fillna(0)
    
    # ---- range取得 ----
    if selected_detector not in config:
        st.warning("detector設定なし")
        st.stop()

    conf = config[selected_detector]

    # --- 表示scale選択 -------
      
    scale_mode = st.radio(
        f"Scale",
        ["log","linear"],
        horizontal=True,
        key=f"scale_{selected_detector}_timing_vs_charge"
        )

    try:
        x_min, x_max = conf["range"][0]
        y_min, y_max = conf["range"][1]
    except Exception as e:
        st.error(f"range設定が不正: {e}")
        st.stop()

    # ---- Timing cut ----
    t_min, t_max = st.slider("Timing select range",
                             float(x_min),float(x_max),(float(x_min), float(x_max)),
                             key=f"timing_cut_{selected_detector}"
                             )
    df_cut = df_det[
        (df_det["x_bin"] >= t_min) &
        (df_det["x_bin"] <= t_max)]

    # ---- データ再集計 ----
    d_agg = df_cut.groupby(["x_bin", "y_bin"])["z"].sum().reset_index()
    pivot = d_agg.pivot(index="y_bin", columns="x_bin", values="z").fillna(0)

    # --- scale calc-- this should be done after "d_agg"
    if scale_mode == "log":
        from matplotlib.colors import LogNorm
        norm = LogNorm()
        z = pivot.values.astype(float)
        z[z <= 0] = np.nan
    else:
        norm = None
        z = pivot.values

 #    ---- 描画 ----
 #   import matplotlib.pyplot as plt
 #   import plotly.express as px
    import plotly.graph_objects as go

    x_vals = pivot.columns.values
    y_vals = pivot.index.values

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=x_vals,
            y=y_vals,
            colorscale="jet",
            colorbar=dict(title="Counts")
            )
        )

    fig.update_layout(
        title=f"RUN{run_id} {selected_detector} Timing vs Charge",
        xaxis_title="Timing",
        yaxis_title="Charge",
        plot_bgcolor="white",
        height=600,
        font=dict(size=18)
        )
    fig.update_xaxes(
        showgrid=False,
        title_font=dict(size=20),
        tickfont=dict(size=16)
        )
    fig.update_yaxes(
        showgrid=False,
        title_font=dict(size=20),
        tickfont=dict(size=16)
        )


    st.plotly_chart(fig, use_container_width=True)



with tab4:
    try:
        df = pd.read_parquet(
            manifest["chargedist"]["data_path"],
            engine="pyarrow")
    except Exception as e:
        st.error(f"読み込み失敗: {e}")
        st.stop()

    # ---- config 読み込み ----
    try:
        with open(manifest["chargedist"]["config_path"]) as f:
            config = json.load(f)
    except Exception as e:
        st.error(f"config読み込み失敗: {e}")
        st.stop()

    valid_hist_names = df["hist_name"].dropna().unique()
    ppac_list = sorted(set(h[:-1] for h in valid_hist_names))   #"sr91","sr92"

    # ---- select detectors -----
    selected_detector = st.selectbox("Detector", ppac_list,key="chargedist")
    # ---- スケール ----
    scale_mode = st.radio( "Scale",["linear", "log"],horizontal=True,key=f"scale_chargedist")

    col1, col2 = st.columns(2)

    histx = df["colName"].dropna().unique() # charge0, charge1, charge2
    colors = ["tab:blue", "tab:orange", "tab:green"]

    for col, axis in zip([col1, col2], ["x", "y"]):
        with col:
            detector_plane = f"{selected_detector}{axis}"
            fig, ax = plt.subplots()
            for ch, color in zip(histx, colors):
                d = df[
                    (df["hist_name"] == detector_plane) &
                    (df["colName"] == ch)
                    ]
                
                if d.empty:
                    st.warning("No hist data")
                    continue
                # ---- range取得 ----
                if detector_plane not in config:
                    st.warning("detector設定なし")
                    st.stop()

                conf = config[detector_plane]
                x_min, x_max = conf["range"]
             
            # ---- 描画 ----
                ax.plot(
                    d["x_bin"],   # または bin中心
                    d["z"],       # カウント
                    label=ch,
                    color=color,
                    linewidth=2
                    )
                
            if scale_mode == "log":
                ax.set_yscale("log")
            ax.set_xlim(x_min,x_max)
            ax.set_title("RUN"+run_id+" "+detector_plane+" Charge0,1,2")
            ax.set_yscale(scale_mode)
            ax.set_xlabel("Charge")
            # ---- 凡例 ----
            ax.legend()
            st.pyplot(fig)



with tab5:
    try:
        df = pd.read_parquet(
            manifest["dqdx"]["data_path"],
            engine="pyarrow")
    except Exception as e:
        st.error(f"読み込み失敗: {e}")
        st.stop()

    # ---- config 読み込み ----
    try:
        with open(manifest["dqdx"]["config_path"]) as f:
            config = json.load(f)
    except Exception as e:
        st.error(f"config読み込み失敗: {e}")
        st.stop()

    valid_hist_names = df["hist_name"].dropna().unique()
    ppac_list = sorted(set(h[:-1] for h in valid_hist_names))   #"sr91","sr92"

    # ---- select detectors -----
    selected_detector = st.selectbox("Detector", ppac_list,key="dqdx")

    col1, col2 = st.columns(2)

    histx = df["colName"].dropna().unique() # q0q1

    for col, axis in zip([col1, col2], ["x", "y"]):
        with col:
            detector_plane = f"{selected_detector}{axis}"

            fig, ax = plt.subplots()

            for ch, color in zip(histx, colors):
                d = df[(df["hist_name"] == detector_plane) &(df["colName"] == ch)]

                if d.empty:
                    continue
                # ---- range取得 ----
                if detector_plane not in config:
                    st.warning("detector設定なし")
                    st.stop()

                conf = config[detector_plane]
                x_min, x_max = conf["range"]
             
            # ---- 描画 ----
                ax.plot(
                    d["x_bin"],   # または bin中心
                    d["z"],       # カウント
                    linestyle="-",
                    label=ch,
                    color="red",
                    linewidth=2.0
                    )
                
            if scale_mode == "log":
                ax.set_yscale("log")
            ax.set_xlim(x_min,x_max)
            ax.set_title("RUN"+run_id+" "+detector_plane)
            ax.set_yscale(scale_mode)
            ax.set_xlabel("(Q0-Q1)/(Q0+Q1)")
            # ---- 凡例 ----
            ax.legend()
            st.pyplot(fig)