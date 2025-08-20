# 📘 User Guide: Billing Report Validation (`billing.py`)

This guide explains how to generate and validate **Billing & Invoice Reports** for a specific tenant using the **CMS Production portal** and the Python script **`billing.py`**.

---

## 🔹 Prerequisites

1. **Access to CMS Production**  
   - Valid login credentials for the [CMS Production Portal].

2. **Python Installed**  
   - Python **3.8+** must be installed.  
   - Verify installation by running:
     ```bash
     python --version
     ```

3. **Script File**  
   - Ensure `billing.py` exists at:
     ```
     C:\Automation - Report Validation\billing.py
     ```

---

## 🔹 Step 1: Login to CMS Production

1. Open the **CMS Production portal** in your browser.  
2. Log in using your **username and password**.  
3. Navigate to the correct **Tenant** for which you want to validate billing data.  
   - Example:  
     ```
     Tenant = chargezone-b2c 
     MongoID = 62987db08f88870e6524d06a
     ```

---

## 🔹 Step 2: Download the Billing & Invoice Report

1. From the selected tenant’s dashboard, go to:  
   **Billing & Invoicing → Invoice Report**  
2. Select the required **Date Range**:
   - `1 Day`
   - `15 Days`
   - `30 Days`
   *(Choose based on your requirement)*  
3. Click **Download** to save the Excel report.  
   - Example file path:  
     ```
     C:\Data_Report\Billing And Invoice (11).xlsx
     ```

---

## 🔹 Step 3: Run the Billing Validation Script

1. Open **PowerShell**.  
2. Navigate to the script location:
   ```powershell
   cd "C:\Automation - Report Validation"
   Run the script: py billing.py

## 🔹 Step 4: Provide Required Inputes

The script will ask for four inputs:

Prompt	Example Input	Explanation
```
Enter the tenant ID:	62987db08f88870e6524d06a	MongoID of the tenant (from CMS system/MongoDB Database).
Enter the start date (YYYY-MM-DD):	2025-08-01	Same start date as used in CMS Invoice Report.
Enter the end date (YYYY-MM-DD):	2025-08-15	Same end date as used in CMS Invoice Report.
Enter the path of the Excel file:	C:\Data_Report\Billing And Invoice (11).xlsx	File path of downloaded CMS Excel report. 
```
⚠️ Important Notes:

👉 The date range entered in the script must match the one selected while downloading the CMS report.

👉 The tenant MongoID must match the tenant for which the CMS report was downloaded.

## 🔹 Step 5: Example Run
```
PS C:\Automation - Report Validation> py billing.py
Enter the tenant ID: 62987db08f88870e6524d06a
Enter the start date (YYYY-MM-DD): 2025-08-01
Enter the end date (YYYY-MM-DD): 2025-08-15
Enter the path of the Excel file: C:\Data_Report\Billing And Invoice (11).xlsx
```

## 🔹 Step 6: Example Run

The script validates the downloaded invoice report against the tenant’s billing data.

Output will be displayed in the terminal (or saved to a file, depending on script logic).

✅ Process Summary
```
1. Login to CMS Production.

2. Select tenant → Go to Billing & Invoice → Invoice Report.

3. Choose date range → Download Excel report.

4. Run billing.py → Enter Tenant ID, Date Range, Excel File path.

5. Validate results.
```
