# start_bv.py
import os
import uvicorn
from pathlib import Path
from dotenv import load_dotenv
from buenavista.adapter.duckdb import DuckDBAdapter
from buenavista.http import create_app

load_dotenv()

# 1. 从 .env 读取配置
data_dir = Path(os.getenv("DUCKDB_DATA_DIR", "./data"))
db_files = os.getenv("DUCKDB_FILES", "").split(",")
host = os.getenv("DUCKDB_SERVER_HOST", "0.0.0.0")
port = int(os.getenv("DUCKDB_SERVER_PORT", "5433"))

# 2. 创建内存主库
adapter = DuckDBAdapter(":memory:")

# 3. 挂载 .env 中指定的 duckdb 文件
for name in db_files:
    name = name.strip()
    if not name:
        continue
    db_file = data_dir / name
    if db_file.exists():
        alias = db_file.stem
        adapter.execute(f"ATTACH '{db_file}' AS {alias}")
        print(f"Attached {db_file} as {alias}")
    else:
        print(f"Warning: {db_file} not found, skipped")

# 4. 创建并运行服务
app = create_app(adapter)

if __name__ == "__main__":
    uvicorn.run(app, host=host, port=port)