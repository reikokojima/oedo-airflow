
from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI
from fastapi.responses import RedirectResponse

# 1. FastAPIアプリを作成
app = FastAPI(title="Dashboard App")

@app.get("/dashboard")
def redirect_to_streamlit():
    # Streamlitが動いているURL（ポートフォワード越し）
    streamlit_url = "http://localhost:8501"
    # 別タブで開くスクリプトを返す（FastAPI形式）
    from fastapi.responses import HTMLResponse
    html_content = f"""
    <html>
        <body>
            <script>
                window.open('{streamlit_url}', '_blank');
                window.location.href = '/home';
            </script>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content)

# 2. プラグインとして登録
class AirflowDashboardPlugin(AirflowPlugin):
    name = "dashboard_plugin"
    
    # Airflow 3以降の新しいUI統合方法
    fastapi_apps = [
        {
            "app": app,
            "url_prefix": "/streamlit",
            "name": "Dashboard",  # メニューに表示される名前
        }
    ]
