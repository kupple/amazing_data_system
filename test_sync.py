import requests
import sys
sys.path.insert(0, r"C:\Users\mubin\Desktop\amazing_data_system")

# First, check if API is running
try:
    r = requests.get("http://localhost:8000/health", timeout=5)
    print("API is running:", r.text)
except:
    print("API not running, please start it first with: python main.py --mode api")
    sys.exit(1)

# Trigger sync
r = requests.post(
    "http://localhost:8000/api/sync/trigger",
    json={"data_type": "trading_calendar", "force": True}
)
print("Response:", r.text)
