"""
DuckDB Server - 使用 checkpoint 方式
"""
import duckdb
import threading
import time

class DuckDBServer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path, read_only=False)
        self.running = True
        self.checkpoint_interval = 30  # 每30秒checkpoint一次
        
    def checkpoint(self):
        """定期checkpoint"""
        while self.running:
            try:
                self.conn.execute("CHECKPOINT;")
                print(f"[{time.strftime('%H:%M:%S')}] Checkpoint done")
            except Exception as e:
                print(f"Checkpoint error: {e}")
            time.sleep(self.checkpoint_interval)
    
    def run(self):
        print(f"DuckDB Server running on: {self.db_path}")
        print("Press Ctrl+C to stop")
        
        # 启动 checkpoint 线程
        checkpoint_thread = threading.Thread(target=self.checkpoint, daemon=True)
        checkpoint_thread.start()
        
        # 保持运行
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.running = False
            self.conn.close()
            print("Server stopped")

if __name__ == "__main__":
    server = DuckDBServer("C:\\Users\\mubin\\.openclaw\\workspace\\data\\baostock_full.duckdb")
    server.run()
