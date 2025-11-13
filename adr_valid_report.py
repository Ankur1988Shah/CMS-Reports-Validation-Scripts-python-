import subprocess
import os
from datetime import datetime, timedelta
from tenant_list import TENANTS
import smtplib
from email.message import EmailMessage
import pandas as pd  #  New import to check Excel data

# === CONFIG ===
BASE_PATH = r"C:\Users\TCZ\Desktop\Wallet Reconcilation Monthwise -Final"
ADR_SCRIPT = r"C:\Automation - Report Validation\adr2.py"
SENDER_EMAIL = "ankur.shah@chargezone.co.in"
APP_PASSWORD = None   #  Keep None for test mode (no real password)
RECEIVER_EMAIL = "ankur.shah@chargezone.co.in"

# === DATE SETUP ===
yesterday = datetime.now() - timedelta(days=1)
start_date = yesterday.strftime("%Y-%m-%d")
end_date = yesterday.strftime("%Y-%m-%d")
date_str = yesterday.strftime("%d-%b-%Y")

# === REPORT GENERATION LOOP ===
generated_files = []

for tenant_name, tenant_id in TENANTS.items():
    print(f"\n▶ Generating report for {tenant_name} ({tenant_id})")

    # Input Excel (already downloaded)
    EXCEL_PATH = os.path.join(BASE_PATH, f"ADR_{tenant_name}_{yesterday.strftime('%Y-%m-%d')}.xlsx")

    if not os.path.exists(EXCEL_PATH):
        print(f" Excel not found for {tenant_name}, skipping...")
        continue

    #  Check if Excel is empty (no data rows)
    try:
        df = pd.read_excel(EXCEL_PATH)
        if df.empty:
            print(f"  Excel is empty for {tenant_name}, skipping validation...")
            continue
    except Exception as e:
        print(f" Failed to read {tenant_name} Excel file: {e}")
        continue

    # Output file path (tenant name included)
    output_name = f"ADR_{tenant_name}_{date_str}_Validated.xlsx"
    output_path = os.path.join(BASE_PATH, output_name)

    # Run adr.py validation safely
    try:
        subprocess.run([
            "python", ADR_SCRIPT,
            tenant_id,
            start_date,
            end_date,
            EXCEL_PATH
        ], check=True)
    except subprocess.CalledProcessError:
        print(f" Validation failed for {tenant_name}. Skipping...")
        continue

    # Move the generated report to new name (tenant-wise)
    default_report = os.path.join(
        os.path.expanduser("~"),
        "Desktop",
        "Reports_Validation_Montly",
        "All_Data_Report.xlsx"
    )

    if os.path.exists(default_report):
        os.replace(default_report, output_path)
        generated_files.append(output_path)
        print(f"✅ {tenant_name} report validated and saved: {output_path}")
    else:
        print(f"⚠️ Validation report not found for {tenant_name}")

# === EMAIL ALL REPORTS TOGETHER ===
if generated_files:
    print("\n Preparing email with all reports...")

    msg = EmailMessage()
    msg["Subject"] = f"ADR Validation Reports - {date_str}"
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL
    msg.set_content(
        f"Hello,\n\nPlease find attached ADR validation reports for {date_str}.\n\nRegards,\nAutomation Bot"
    )

    for report in generated_files:
        with open(report, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype="application",
                subtype="xlsx",
                filename=os.path.basename(report)
            )

    # --- EMAIL SEND / SAFE TEST MODE ---
    if APP_PASSWORD:
        with smtplib.SMTP("smtp.office365.com", 587) as smtp:
            smtp.starttls()
            smtp.login(SENDER_EMAIL, APP_PASSWORD)
            smtp.send_message(msg)
        print("✅ All reports emailed successfully!")
    else:
        temp_eml = os.path.join(BASE_PATH, f"TestMail_{date_str}.eml")
        with open(temp_eml, "wb") as f:
            f.write(bytes(msg))
        print(f" Test mode: Email content saved instead of sending.\n {temp_eml}")
else:
    print(" No reports were generated to send.")
