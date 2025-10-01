# import pandas as pd

# def insert_exam_absences(file_path, engine, employees_table="employees2"):
#     df_emps = pd.read_sql(f"SELECT employee_id, full_name FROM {employees_table}", engine)
#     name_to_id = dict(zip(df_emps['full_name'], df_emps['employee_id']))
#     df = pd.read_csv(file_path)

#     rows_to_insert = []

#     for idx, row in df.iterrows():
#         event_title = row['Summary']
#         event_date = pd.to_datetime(row['Start']).date() if pd.notnull(row['Start']) else None
#         check_in_time = pd.to_datetime(row['Start']) if pd.notnull(row['Start']) else None
#         check_out_time = pd.to_datetime(row['End']) if pd.notnull(row['End']) else None
#         attendees_raw = row.get('Attendees Names', '')
#         attendees_str = str(attendees_raw) if pd.notnull(attendees_raw) else ''
#         attendees = [a.strip() for a in attendees_str.split(",") if a.strip() and attendees_str.lower() != "nan"]
                
#         for attendee in attendees:
#             employee_id = name_to_id.get(attendee)
#             rows_to_insert.append({
#                 "employee_id": employee_id, 
#                 "event_title": event_title,
#                 "event_date": event_date,
#                 "check_in_time": check_in_time,
#                 "check_out_time": check_out_time,
#                 "attended": 'Y'
#             })

#     return rows_to_insert

import pandas as pd

def insert_exam_absences(file_path, engine, employees_table="employees2"):
    df_emps = pd.read_sql(f"SELECT employee_id, full_name FROM {employees_table}", engine)
    name_to_id = dict(zip(df_emps['full_name'], df_emps['employee_id']))
    df = pd.read_csv(file_path)

    rows_to_insert = []

    for idx, row in df.iterrows():
        event_title = row['Summary']
        event_date = pd.to_datetime(row['Start']).date() if pd.notnull(row['Start']) else None
        check_in_time = pd.to_datetime(row['Start']) if pd.notnull(row['Start']) else None
        check_out_time = pd.to_datetime(row['End']) if pd.notnull(row['End']) else None
        attendees_raw = row.get('Attendees Names', '')
        attendees_str = str(attendees_raw) if pd.notnull(attendees_raw) else ''
        attendees = [a.strip() for a in attendees_str.split(",") if a.strip() and attendees_str.lower() != "nan"]
                
        for attendee in attendees:
            employee_id = name_to_id.get(attendee)
            if not employee_id:
                continue  # Optionally log skipped names
            duration = None
            if check_in_time is not None and check_out_time is not None:
                delta = check_out_time - check_in_time
                duration = round(delta.total_seconds() / 3600, 2)
            rows_to_insert.append({
                "employee_id": employee_id,
                "absence_date": event_date,
                "absence_type": "exam",
                "reason": event_title,
                "duration": duration
            })

    return pd.DataFrame(rows_to_insert)
