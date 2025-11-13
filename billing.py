import os
import pandas as pd
from pymongo import MongoClient
from bson import ObjectId
import pytz
from datetime import datetime


# Convert IST to UTC
def convert_ist_to_utc(ist_datetime_str, is_end_date=False):
    ist = pytz.timezone('Asia/Kolkata')
    ist_datetime = datetime.strptime(ist_datetime_str, "%Y-%m-%d")
    ist_datetime = ist.localize(ist_datetime)
    if is_end_date:
        ist_datetime = ist_datetime.replace(hour=23, minute=59, second=59)
    utc_datetime = ist_datetime.astimezone(pytz.utc)
    return utc_datetime.strftime("%Y-%m-%d %H:%M:%S")

# Convert UTC to IST
def convert_utc_to_ist(utc_datetime_str):
    utc = pytz.utc
    utc_datetime = datetime.strptime(utc_datetime_str, "%Y-%m-%d %H:%M:%S")
    utc_datetime = utc.localize(utc_datetime)
    ist = pytz.timezone('Asia/Kolkata')
    ist_datetime = utc_datetime.astimezone(ist)
    return ist_datetime.strftime("%Y-%m-%d %H:%M:%S")
# MongoDB connection and data fetch function
def fetch_data_from_mongodb(tenant_id, start_date, end_date):
    client = MongoClient("mongodb+srv://Ankur_shah:ankurshah123@cluster1.0pycd.mongodb.net/chargezoneprod")
    db = client['chargezoneprod']
    collection = db['chargerbookings']

    pipeline = [
        {"$match": {
            "status": "completed",
            "tenant": ObjectId(tenant_id),
            "booking_stop": {
                "$gte": pd.to_datetime(start_date),
                "$lt": pd.to_datetime(end_date)
            },
            "invoice": {"$exists": True}
        }
    }, 
    {
        "$lookup": {
            "from": "invoices",
            "localField": "invoice",
            "foreignField": "_id",
            "as": "invoice_data"
        }
    },
    {
        "$unwind": {
            "path": "$invoice_data",
            "preserveNullAndEmptyArrays": True
        }
    },
    { "$match": { "invoice_data.invoice_no": { "$exists": True } }},
    {
        "$lookup": {
            "from": "users",
            "localField": "customer_user_booked",
            "foreignField": "_id",
            "as": "user_data"
        }
    },
    {
        "$unwind": {
            "path": "$user_data",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$lookup": {
            "from": "chargers",
            "localField": "charger",
            "foreignField": "_id",
            "as": "chargerData"
        }
    },
    {
        "$unwind": {
            "path": "$chargerData",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$lookup": {
            "from": "chargingstations",
            "localField": "chargerData.charging_station",
            "foreignField": "_id",
            "as": "stationData"
        }
    },
    {
        "$unwind": {
            "path": "$stationData",
            "preserveNullAndEmptyArrays": True
        }
    },
  
    {
        "$lookup": {
            "from": "ocpicredentials",
            "localField": "ocpiCredential",
            "foreignField": "_id",
            "as": "ocpiData"
        }
    },
    {
        "$unwind": {
            "path": "$ocpiData",
            "preserveNullAndEmptyArrays": True
        }
    },
   
    {
        "$lookup": {
            "from": "companies",
            "localField": "tenant",
            "foreignField": "_id",
            "as": "tenantData"
        }
    },
    {
        "$unwind": {
            "path": "$tenantData",
            "preserveNullAndEmptyArrays": True
        }
    },
   
    {
        "$project": {
            "booking_id": {"$toString": "$_id"},
            "customer_name": "$user_data.name",
            "mobile_no": "$user_data.phone",
            "customer_id": "$user_data._id",
            "our_gst_number": "$invoice_data.gst_used",
            "station_id": "$stationData.station_id",
            "station_name": "$stationData.name",
            "ocpp_id": "$chargerData.charger_id",
            "party_id": "$ocpiData.roles.party_id",
            "transaction_id": "$transaction_id",
            "RFID/idTag/Autocharge": "$idTag",
            "booking_type": "$booking_type",
            "boooking_schedule_datetime_IST": {
                "$dateToString": {
                    "format": "%Y-%m-%d %H:%M:%S",
                    "date": "$schedule_datetime",
                    "timezone": "+05:30"
                }
             },
            "sesson_start_datetime_IST": {
                "$dateToString": {
                    "format": "%Y-%m-%d %H:%M:%S",
                    "date": "$booking_start",
                    "timezone": "+05:30"
                }
            },
            "sesson_stop_datetime_IST": {
                "$dateToString": {
                    "format": "%Y-%m-%d %H:%M:%S",
                    "date": "$booking_stop",
                    "timezone": "+05:30"
                }
            },

            "meter_start_reading": "$meterstart",
            "meter_stop_reading": "$meterstop",
            # "unit_consumed": "$unit_consumed",
            "invoice_no": "$invoice_data.invoice_no",
            "energy_rate": { "$round": ["$invoice_data.price_per_unit", 2] },
            "discount": { "$round": ["$invoice_data.chargecoins_used", 3] },
            "cashback": "$invoice_data.cashback",
            "net_amount": { "$round": ["$invoice_data.service_charge", 3] },
            "gst": { "$round": ["$invoice_data.gst", 2] },
            "taxable_amount": { "$round": ["$invoice_data.subtotal", 3] },
            "total_amount_paid": { "$round": ["$invoice_data.total_amount", 3] },
            "irn": "$invoice_data.logitax.Irn",
            "ack_no": "$invoice_data.logitax.AckNo",
            "ack_dt": "$invoice_data.logitax.AckDt",
            "session_unit_consumption": {
                "$divide": [{ "$subtract": ["$meterstop", "$meterstart"] }, 1000]
            },
            # "external_charger": "$chargerData.is_external_charger",
            # "connector_name": {
            #     "$function": {
            #         "body": "function(connectorId) { if(typeof connectorId === 'number' && connectorId > 0) { return String.fromCharCode(64 + connectorId); } return null; }",
            #         "args": ["$connectorId"],
            #         "lang": "js"
            #     }
            # },
            # "charger_vendor": "$vendorData.name",
           
            #  "schedule_datetime": 1,
            #  "booking_start": 1,
            #  "booking_stop": 1,
            
            # "session_duration": {
            #     "$let": {
            #         "vars": {
            #             "durationMs": { "$subtract": ["$booking_stop", "$booking_start"] }
            #         },
            #         "in": {
            #             "$concat": [
            #                 { "$cond": [{ "$lt": [{ "$floor": { "$divide": ["$$durationMs", 3600000] } }, 10] }, { "$concat": ["0", { "$toString": { "$floor": { "$divide": ["$$durationMs", 3600000] } } }] }, { "$toString": { "$floor": { "$divide": ["$$durationMs", 3600000] } } }] },
            #                 ":",
            #                 { "$cond": [{ "$lt": [{ "$floor": { "$mod": [{ "$divide": ["$$durationMs", 60000] }, 60] } }, 10] }, { "$concat": ["0", { "$toString": { "$floor": { "$mod": [{ "$divide": ["$$durationMs", 60000] }, 60] } } }] }, { "$toString": { "$floor": { "$mod": [{ "$divide": ["$$durationMs", 60000] }, 60] } } }] },
            #                 ":",
            #                 { "$cond": [{ "$lt": [{ "$floor": { "$mod": [{ "$divide": ["$$durationMs", 1000] }, 60] } }, 10] }, { "$concat": ["0", { "$toString": { "$floor": { "$mod": [{ "$divide": ["$$durationMs", 1000] }, 60] } } }] }, { "$toString": { "$floor": { "$mod": [{ "$divide": ["$$durationMs", 1000] }, 60] } } }] }
            #             ]
            #         }
            #     }
            # },
           
            # "reason_stop": "$stop_reason",
            # "session_stop_by": "$session_stop_by",
            # "energy_rate": "$invoice_data.price_per_unit",
          
            # "company_GST": "$invoice_data.gst_used",
            #"discount": "$invoice_data.chargecoins_used",
            "discount": {
                    "$cond": {
                        "if": { 
                            "$gte": ["$booking_stop", datetime(2025, 9, 1, 0, 0, 0)]  # >= 1 Sep 2025
                        },
                        "then": { 
                            "$round": [
                                { "$divide": ["$invoice_data.chargecoins_used", 5] },
                                2
                            ]
                        },
                        "else": { 
                            "$round": ["$invoice_data.chargecoins_used", 2]
                     }
                 }
            },
          
            #"net_amount": "$invoice_data.service_charge",
           
            #"gst": "$invoice_data.gst",
           
            #"taxable_amount": "$invoice_data.subtotal",
            
            #"total_amount_paid": "$invoice_data.total_amount",
            
            
            # "invoice_type": {
            #     "$cond": {
            #         "if": {
            #             "$and": [
            #                 { "$eq": ["$invoice_data.einvoice_generated", True] },
            #                 { "$ne": [{ "$ifNull": ["$invoice_data.logitax", None] }, None] }
            #             ]
            #         },
            #         "then": "E-Invoice",
            #         "else": "Tax-Invoice"
            #     }
            # },
          
            # "make(model)": {
            #     "$concat": ["$vehicleData.make", "(", "$vehicleData.model", ")"]
            # },
           
            
            #"customer_GST": "$user_data.customer_gst_array.gstin",
            # "min_temperature": "$min_temperature",
            # "max_temperature": "$max_temperature",
            # "vehicle_registration_number": "$vehicleData.vin_num",
            # "cpo_name": {
            #     "$cond": {
            #         "if": { "$eq": ["$external_charger", False] },
            #         "then": "ChargeZone-B2C",
            #         "else": "$ocpiData.partner_name"
            #     }
            # },
            # "initiated_by": "$ocpifleetData.initiated_by",
            # "fleet_name": "$ocpifleetData.initiator_name",
            # "ownership_type": "$chargerData.ownership_type",
            #"connector_type": "$connectorData.name",
            #"station_access_type": "$stationData.access_type",
            "tenant_name": "$tenantData.name",
            "discounted_percentage": "$invoice_data.cashback_percent"
        }
    }


    ]

    #return list(collection.aggregate(pipeline))
    return list(collection.aggregate(pipeline, allowDiskUse=True))


# Load Excel file with header row at index 2 (row 3)
def load_excel_file(file_path):
    return pd.read_excel(file_path, header=2)


# Calculate GST(INR)
def calculate_gst_inr(excel_data):
    excel_data.columns = excel_data.columns.str.strip()
    igst_col = next((col for col in excel_data.columns if 'igst' in col.lower()), None)
    cgst_col = next((col for col in excel_data.columns if 'cgst' in col.lower()), None)
    sgst_col = next((col for col in excel_data.columns if 'sgst' in col.lower()), None)

    if igst_col and cgst_col and sgst_col:
        excel_data[igst_col] = pd.to_numeric(excel_data[igst_col], errors='coerce').fillna(0)
        excel_data[cgst_col] = pd.to_numeric(excel_data[cgst_col], errors='coerce').fillna(0)
        excel_data[sgst_col] = pd.to_numeric(excel_data[sgst_col], errors='coerce').fillna(0)
        excel_data['GST(INR)'] = (
            excel_data[igst_col] + excel_data[cgst_col] + excel_data[sgst_col]
        )
    else:
        print("Error: IGST, CGST, or SGST columns are missing in the Excel file.")
        return None

    return excel_data

# Count sessions using booking_id
def count_sessions(mongo_data, excel_data):
    total_mongo_sessions = len(mongo_data)

    # if 'Booking Id' in excel_data.columns:
    #     mongo_ids = set(str(item['booking_id']) for item in mongo_data if 'booking_id' in item)
    #     excel_ids = set(str(val).strip() for val in excel_data['Booking Id'].dropna())
    #     matched_ids = mongo_ids.intersection(excel_ids)
    #     total_excel_sessions = len(matched_ids)
    # else:
    #     total_excel_sessions = 0

    if 'Invoice Number' in excel_data.columns:
        mongo_invoices = set(str(item['invoice_no']) for item in mongo_data if 'invoice_no' in item)
        excel_invoices = set(str(val).strip() for val in excel_data['Invoice Number'].dropna())
        matched_invoices = mongo_invoices.intersection(excel_invoices)
        total_excel_sessions = len(matched_invoices)
    else:
        total_excel_sessions = 0


    return total_mongo_sessions, total_excel_sessions

# Compare totals between MongoDB and Excel
def compare_total_units_consumed(mongo_data, excel_data):
    mongo_df = pd.DataFrame(mongo_data)

    # Normalize booking_id for clean comparison
    # mongo_df['booking_id_str'] = mongo_df['booking_id'].astype(str).str.strip()
    # excel_data['booking_id_str'] = excel_data['Booking Id'].astype(str).str.strip()
    mongo_df['invoice_no_str'] = mongo_df['invoice_no'].astype(str).str.strip()
    excel_data['invoice_no_str'] = excel_data['Invoice Number'].astype(str).str.strip()


    # NEW: Identify mismatched records by comparing values
    merged_df = pd.merge(
        mongo_df,
        excel_data,
        how='inner',
        on='invoice_no_str',
        suffixes=('_mongo', '_excel')
    )

    fields_to_compare = [
        ('session_unit_consumption', 'Unit Consumed (kWh)'),
        ('energy_rate', 'Price Per Unit'),
        ('net_amount', 'Basic'),
        ('discount', 'Discount'),
        ('taxable_amount', 'Taxable Amount'),
        ('gst', 'GST(INR)'),
        ('total_amount_paid', 'Total Invoice Value')
    ]

   

    mismatch_mask = False
    for mongo_col, excel_col in fields_to_compare:
        mongo_vals = pd.to_numeric(merged_df[mongo_col], errors='coerce').fillna(0)
        excel_vals = pd.to_numeric(merged_df[excel_col], errors='coerce').fillna(0)
        mismatch_mask |= (mongo_vals.round(2) != excel_vals.round(2))

    # ✅ After loop ends — apply filter and format
    missed_records_df = merged_df[mismatch_mask].copy()

    # ✅ Rename Excel columns with (CMS) suffix
    rename_map = {
        'session_unit_consumption': 'Session Unit Consumption (kWh) (DB)',
        'energy_rate': 'Energy Rate (DB)',
        'net_amount': 'Net Amount(INR) (DB)',
        'discount': 'Discount (DB)',
        'taxable_amount': 'Taxable Amount (DB)',
        'gst': 'GST(INR) (DB)',
        'total_amount_paid': 'Total Amount Paid(INR) (DB)',

        'Unit Consumed (kWh)': 'Unit Consumed (kWh) (CMS)',
        'Price Per Unit': 'Price Per Unit (CMS)',
        'Basic': 'Basic (CMS)',
        'Discount': 'Discount (CMS)',
        'Taxable Amount': 'Taxable Amount (CMS)',
        'GST(INR)': 'GST(INR) (CMS)',
        'Total Invoice Value': 'Total Invoice Value (CMS)'
    }

    missed_records_df.rename(columns=rename_map, inplace=True)

    # ✅ Define column pairs with DB, CMS, and DIFF
    column_pairs = [
        ('Session Unit Consumption (kWh) (DB)', 'Unit Consumed (kWh) (CMS)', 'Diff (Session Unit Consumption)'),
        ('Energy Rate (DB)', 'Price Per Unit (CMS)', 'Diff (Energy Rate)'),
        ('Net Amount(INR) (DB)', 'Basic (CMS)', 'Diff (Net Amount)'),
        ('Discount (DB)', 'Discount (CMS)', 'Diff (Discount)'),
        ('Taxable Amount (DB)', 'Taxable Amount (CMS)', 'Diff (Taxable Amount)'),
        ('GST(INR) (DB)', 'GST(INR) (CMS)', 'Diff (GST)'),
        ('Total Amount Paid(INR) (DB)', 'Total Invoice Value (CMS)', 'Diff (Total Amount Paid)')
    ]

    # ✅ Calculate differences
    for db_col, cms_col, diff_col in column_pairs:
        missed_records_df[diff_col] = missed_records_df.apply(
            lambda row: (
                round(
                    pd.to_numeric(row[db_col], errors='coerce') - pd.to_numeric(row[cms_col], errors='coerce'),
                    4
                ) if pd.notnull(row[db_col]) and pd.notnull(row[cms_col]) else 'Mismatch'
            ),
            axis=1
        )

    # ✅ Reorder columns: other fields first, then grouped: DB → CMS → Diff
    used_cols = [col for triple in column_pairs for col in triple]
    other_cols = [col for col in missed_records_df.columns if col not in used_cols]

    final_cols = other_cols.copy()
    for db_col, cms_col, diff_col in column_pairs:
        final_cols.extend([db_col, cms_col, diff_col])

    missed_records_df = missed_records_df[final_cols]




    
    #-------------------

    # Start with identifiers
    existing_cols = list(missed_records_df.columns)
    ordered_cols = []
    for col in ['invoice_no', 'station_id', 'ocpp_id', 'transaction_id']:
        if col in existing_cols:
            ordered_cols.append(col)

    # Add DB + CMS column pairs
    for db_col, cms_col, _ in column_pairs:

        if db_col in existing_cols:
            ordered_cols.append(db_col)
        if cms_col in existing_cols:
            ordered_cols.append(cms_col)

    # Append the rest
    for col in existing_cols:
        if col not in ordered_cols:
            ordered_cols.append(col)

    # Final reorder
    missed_records_df = missed_records_df[ordered_cols]


    #  Mongo totals
    total_mongo_units = mongo_df['session_unit_consumption'].sum()
    total_mongo_price = mongo_df['energy_rate'].sum()
    total_mongo_service_charge = mongo_df['net_amount'].sum()
    total_mongo_chargecoin_used = mongo_df['discount'].sum()
    total_mongo_taxable_amount = mongo_df['taxable_amount'].sum()
    total_mongo_gst = mongo_df['gst'].sum()
    total_mongo_payable_amount = mongo_df['total_amount_paid'].sum()

    # Excel totals
    excel_data['Unit Consumed (kWh)'] = pd.to_numeric(excel_data['Unit Consumed (kWh)'], errors='coerce').fillna(0)
    excel_data['Price Per Unit'] = pd.to_numeric(excel_data['Price Per Unit'], errors='coerce').fillna(0)
    excel_data['Basic'] = pd.to_numeric(excel_data['Basic'], errors='coerce').fillna(0)
    excel_data['Discount'] = pd.to_numeric(excel_data['Discount'], errors='coerce').fillna(0)
    excel_data['Taxable Amount'] = pd.to_numeric(excel_data['Taxable Amount'], errors='coerce').fillna(0)
    excel_data['GST(INR)'] = pd.to_numeric(excel_data['GST(INR)'], errors='coerce').fillna(0)
    excel_data['Total Invoice Value'] = pd.to_numeric(excel_data['Total Invoice Value'], errors='coerce').fillna(0)

    total_excel_units = excel_data['Unit Consumed (kWh)'].sum()
    total_excel_price = excel_data['Price Per Unit'].sum()
    total_excel_net_amount = excel_data['Basic'].sum()
    total_excel_discount = excel_data['Discount'].sum()
    total_excel_taxable_amount = excel_data['Taxable Amount'].sum()
    total_excel_gst = excel_data['GST(INR)'].sum()
    total_excel_payable_amount = excel_data['Total Invoice Value'].sum()

    return (
        total_mongo_units, total_excel_units,
        total_mongo_price, total_excel_price,
        total_mongo_service_charge, total_excel_net_amount,
        total_mongo_chargecoin_used, total_excel_discount,
        total_mongo_taxable_amount, total_excel_taxable_amount,
        total_mongo_gst, total_excel_gst,
        total_mongo_payable_amount, total_excel_payable_amount,
        missed_records_df
    )


# Main
if __name__ == "__main__":
    tenant_id = input("Enter the tenant ID: ")
    start_date_ist = input("Enter the start date (YYYY-MM-DD): ")
    end_date_ist = input("Enter the end date (YYYY-MM-DD): ")
    excel_file_path = input("Enter the path of the Excel file: ")

    
    #from your_fetch_function_file import fetch_data_from_mongodb  # ✅ Your existing import

    start_date_utc = convert_ist_to_utc(start_date_ist)
    end_date_utc = convert_ist_to_utc(end_date_ist, is_end_date=True)

    print(f"\nValidation Date Range: {start_date_utc} to {end_date_utc}\n")

    mongo_data = fetch_data_from_mongodb(tenant_id, start_date_utc, end_date_utc)
    excel_data = load_excel_file(excel_file_path)



      # ---- Print MongoDB Headers ----
    if mongo_data:
        print("\n✅ MongoDB Columns Detected:")
        print(", ".join(pd.DataFrame(mongo_data).columns.tolist()))
    else:
        print("\n⚠️ No data returned from MongoDB!")

    # ---- Debugging: Raw Excel first 5 rows ----
    raw_excel = pd.read_excel(excel_file_path, header=None)
    print("\n📌 First 5 rows of Excel (raw):")
    print(raw_excel.head(5).to_string(index=False))

    # ---- Debugging: Excel Header Row (Row 3) ----
    print("\n📌 Excel Header Row (Row 3):")
    print(raw_excel.iloc[2].tolist())

    # ---- Print Excel Headers detected by pandas ----
    print("\n✅ Excel Columns Detected (Pandas):")
    print(", ".join(excel_data.columns.tolist()))




    excel_data = calculate_gst_inr(excel_data)
    if excel_data is None:
        print("Unable to proceed due to missing columns in Excel.")
        exit(1)

    totals = compare_total_units_consumed(mongo_data, excel_data)
    total_mongo_sessions, total_excel_sessions = count_sessions(mongo_data, excel_data)

    (
        total_mongo_units, total_excel_units,
        total_mongo_price, total_excel_price,
        total_mongo_service_charge, total_excel_net_amount,
        total_mongo_chargecoin_used, total_excel_discount,
        total_mongo_taxable_amount, total_excel_taxable_amount,
        total_mongo_gst, total_excel_gst,
        total_mongo_payable_amount, total_excel_payable_amount,
        missed_records_df
    ) = totals

    print()
    print(f"DB_Data: Total_Sessions = {total_mongo_sessions}")
    print(f"CMS_Excel: Total_Sessions = {total_excel_sessions}")
    print(f"\nDB_Data: Total_Unit_Consumed = {total_mongo_units}")
    print(f"CMS_Excel: Total_Unit_Consumed = {total_excel_units}")
    print(f"\nDB_Data: Total_Price_per_Unit = {total_mongo_price}")
    print(f"CMS_Excel: Total_Energy_Rate = {total_excel_price}")
    print(f"\nDB_Data: Total_Service_Charge = {total_mongo_service_charge}")
    print(f"CMS_Excel: Total_Net_Amount = {total_excel_net_amount}")
    print(f"\nDB_Data: Total_Chargecoin_Used = {total_mongo_chargecoin_used}")
    print(f"CMS_Excel: Total_Discount = {total_excel_discount}")
    print(f"\nDB_Data: Total_Taxable_Amount = {total_mongo_taxable_amount}")
    print(f"CMS_Excel: Total_Taxable_Amount = {total_excel_taxable_amount}")
    print(f"\nDB_Data: Total_GST = {total_mongo_gst}")
    print(f"CMS_Excel: Total_GST = {total_excel_gst}")
    print(f"\nDB_Data: Total_Payable_Amount = {total_mongo_payable_amount}")
    print(f"CMS_Excel: Total_Payable_Amount = {total_excel_payable_amount}\n")

    start_date_ist = convert_utc_to_ist(start_date_utc)
    end_date_ist = convert_utc_to_ist(end_date_utc)

    report_data = {
        "Metric": [
            "Total Sessions",
            "Total Unit Consumed",
            "Total Price per Unit",
            "Total Net Amount",
            "Total Discount",
            "Total Taxable Amount",
            "Total GST",
            "Total Payable Amount"
        ],
        "DB_Data": [
            total_mongo_sessions,
            total_mongo_units,
            total_mongo_price,
            total_mongo_service_charge,
            total_mongo_chargecoin_used,
            total_mongo_taxable_amount,
            total_mongo_gst,
            total_mongo_payable_amount
        ],
        "CMS_Excel": [
            total_excel_sessions,
            total_excel_units,
            total_excel_price,
            total_excel_net_amount,
            total_excel_discount,
            total_excel_taxable_amount,
            total_excel_gst,
            total_excel_payable_amount
        ]
    }

    report_data["Difference"] = [
        report_data["DB_Data"][i] - report_data["CMS_Excel"][i] for i in range(len(report_data["DB_Data"]))
    ]

    for key in ['DB_Data', 'CMS_Excel', 'Difference']:
        report_data[key] = [round(val, 2) if isinstance(val, (int, float)) else val for val in report_data[key]]

    report_df = pd.DataFrame(report_data)
    mongo_df = pd.DataFrame(mongo_data)

    date_range_info = pd.DataFrame({
        "Metric": ["Date Range"],
        "DB_Data": [f"{start_date_ist} - {end_date_ist}"],
        "CMS_Excel": [""],
        "Difference": [""]
    })

    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop", "Reports_Validation_Montly")
    if not os.path.exists(desktop_path):
        os.makedirs(desktop_path)

    output_file_path = os.path.join(desktop_path, "Billing & Invoice Report.xlsx")


    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        # Existing summary and data
        date_range_info.to_excel(writer, index=False, header=False, sheet_name="Billing & Invoice Summary", startrow=0)
        date_range_info.to_excel(writer, index=False, header=False, sheet_name="DB Billing & Invoice Report", startrow=0)
        report_df.to_excel(writer, index=False, sheet_name="Billing & Invoice Summary", startrow=len(date_range_info))
        mongo_df.to_excel(writer, index=False, sheet_name="DB Billing & Invoice Report", startrow=len(date_range_info))

        # Always create 'Missed Records from CMS' sheet
        if missed_records_df.empty:
            pd.DataFrame({"Note": ["No missed records found in DB compared to CMS Excel"]}).to_excel(
                writer, index=False, sheet_name="Discrepant Records(DB vs CMS)"
            )
        else:
            missed_records_df.to_excel(writer, index=False, sheet_name="Discrepant Records(DB vs CMS)")

        # Append uploaded Excel file as a new sheet
        uploaded_excel_name = "CMS_" + os.path.splitext(os.path.basename(excel_file_path))[0]
        uploaded_excel_raw = pd.read_excel(excel_file_path, header=None)
        uploaded_excel_raw.to_excel(writer, index=False, header=False, sheet_name=uploaded_excel_name[:31])

    print(f"Report has been saved as Excel: {output_file_path}")
