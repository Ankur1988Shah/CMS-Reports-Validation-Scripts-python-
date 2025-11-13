import requests
from datetime import datetime, timedelta
import os
from tenant_list import TENANTS

# === CONFIGURATION ===
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI2NThkMTlhNzEwODEwYjliODNiMmEzMDMiLCJpYXQiOjE3NjI5Mzg1ODUsImV4cCI6MTc2NDIzNDU4NSwidHlwZSI6ImFjY2VzcyJ9.EBgX7MMT_EVxjVdgoyu8JC8PHKLVlnU69L7uSWvJrsQ"
BASE_PATH = r"C:\Users\TCZ\Desktop\Wallet Reconcilation Monthwise -Final"
URL = "https://cmsreport.prod.chargecloud.net/report/alldatareport"

HEADERS = {
    "accept": "application/json",
    "authorization": f"Bearer {TOKEN}",
    "content-type": "application/json",
    "origin": "https://app.chargecloud.net",
    "referer": "https://app.chargecloud.net/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# === DATE RANGE (IST TIMEZONE ADJUSTMENT) ===
# CMS report usually expects IST-based day range
yesterday_ist = datetime.now() - timedelta(days=1)
from_date = yesterday_ist.replace(hour=0, minute=0, second=0, microsecond=0)
to_date = yesterday_ist.replace(hour=23, minute=59, second=59, microsecond=999000)

# Convert to ISO format (UTC adjusted)
from_iso = (from_date - timedelta(hours=5, minutes=30)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
to_iso = (to_date - timedelta(hours=5, minutes=30)).strftime("%Y-%m-%dT%H:%M:%S.999Z")

print(f"\n Downloading reports for: {yesterday_ist.strftime('%d-%b-%Y')}")
print(f"From: {from_iso}\nTo:   {to_iso}")

# === DOWNLOAD LOOP ===
for tenant_name, tenant_id in TENANTS.items():
    print(f"\n▶ Downloading ADR for {tenant_name} ({tenant_id})")

    payload = {
        "excel": True,
        "from": from_iso,
        "to": to_iso,
        "report": "alldatareport",
        "status": "completed",
        "tenant": tenant_id
    }

    try:
        response = requests.post(URL, headers=HEADERS, json=payload)

        # Success response check
        if response.status_code in [200, 201] and response.content[:2] != b'{"':
            file_name = f"ADR_{tenant_name}_{yesterday_ist.strftime('%Y-%m-%d')}.xlsx"
            save_path = os.path.join(BASE_PATH, file_name)
            with open(save_path, "wb") as f:
                f.write(response.content)
            print(f"✅ Saved: {save_path}")

        else:
            print(f" Failed for {tenant_name} (Code: {response.status_code})")
            print("Server Response:", response.text[:200])

    except Exception as e:
        print(f"⚠️ Error for {tenant_name}: {e}")

print("\n All downloads complete!")
