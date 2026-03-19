# start_bv.py
import uvicorn
from pathlib import Path
from buenavista.adapter.duckdb import DuckDBAdapter
from buenavista.http import create_app

# 1. 指定主数据库（服务启动时默认连接的库）
adapter = DuckDBAdapter(":memory:")

# 2. 自动扫描 data 目录下所有 .duckdb 文件并挂载
data_dir = Path("data")
if data_dir.exists():
    for db_file in data_dir.glob("*.duckdb"):
        alias = db_file.stem  # 文件名去掉后缀作为别名
        adapter.execute(f"ATTACH '{db_file}' AS {alias}")
        print(f"Attached {db_file} as {alias}")

# 3. 创建并运行服务
app = create_app(adapter)

if __name__ == "__main__":
    # 监听本地 5433 端口
    uvicorn.run(app, host="0.0.0.0", port=5433)