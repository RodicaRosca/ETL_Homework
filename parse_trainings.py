# import pandas as pd
# from datetime import datetime

# df = pd.read_excel('trainings.xlsx', header=None)

# meeting_title = df.iloc[1,1]
# attendees_count = df.iloc[2,1]
# start_time = df.iloc[3,1]
# end_time = df.iloc[4,1]
# meeting_duration = df.iloc[5,1]
# avg_attendance = df.iloc[6,1]

# print("=== Meeting Details ===")
# print(f"Title: {meeting_title}")
# print(f"Start time: {start_time}")
# print(f"End time: {end_time}")
# print(f"Duration: {meeting_duration}")
# print(f"Average attendance time: {avg_attendance}")
# print(f"Reported participants: {attendees_count}")
# print()

# # --- 2. Extract participants table ---
# participants = pd.read_excel('trainings.xlsx', header=9)
# participants = participants[participants['Name'].notna()]

# # --- 3. Helper to parse time columns ---
# def parse_time(time_str):
#     try:
#         return datetime.strptime(time_str, "%m/%d/%y, %I:%M:%S %p")
#     except Exception:
#         return pd.NaT

# # --- 4. Calculate attendance and print results ---
# print("=== Attendees & Attendance Details (First Join / Last Leave) ===")
# for _, row in participants.iterrows():
#     name = row['Name']
#     role = row.get('Role', '')
#     email = row.get('Email', '')
#     join_str = row['First Join']
#     leave_str = row['Last Leave']
#     join_time = parse_time(join_str)
#     leave_time = parse_time(leave_str)
#     if pd.notna(join_time) and pd.notna(leave_time) and leave_time > join_time:
#         delta = leave_time - join_time
#         total_seconds = int(delta.total_seconds())
#         hours = total_seconds // 3600
#         mins = (total_seconds % 3600) // 60
#         secs = total_seconds % 60
#         print(f"- {name} ({role}) | {email} | Attended: {hours}h {mins}m {secs}s (First Join: {join_str}, Last Leave: {leave_str})")
#     else:
#         print(f"- {name} ({role}) | {email} | Could not calculate attendance (missing or invalid timestamps)")


import pandas as pd
from datetime import datetime

def parse_time(time_str):
    try:
        return datetime.strptime(time_str, "%m/%d/%y, %I:%M:%S %p")
    except Exception:
        return pd.NaT

def extract_participants_df(file_path, employees_table, engine, event_title=None, event_date=None):
    df_emps = pd.read_sql(f"SELECT employee_id, email FROM {employees_table}", engine)
    email_to_id = {e.lower(): i for e, i in zip(df_emps['email'], df_emps['employee_id']) if pd.notnull(e)}
    participants = pd.read_excel(file_path, header=9)
    participants = participants[participants['Name'].notna()]
    df = pd.read_excel(file_path, header=None)
    event_title = df.iloc[1,1]
    start_time_str = df.iloc[3,1]
    event_date = pd.to_datetime(start_time_str, format='%m/%d/%y, %I:%M:%S %p').date()
    rows = []
    for _, row in participants.iterrows():
        name = row['Name']
        email = row.get('Email', '')
        check_in_str = row['First Join']
        check_out_str = row['Last Leave']
        email_norm = str(email).strip().lower()
        employee_id = email_to_id.get(email_norm)
        check_in = parse_time(check_in_str)
        check_out = parse_time(check_out_str)
        attended = 'Y' if pd.notna(check_in) and pd.notna(check_out) and check_out > check_in else 'N'
        rows.append({
            "employee_id": employee_id,
            'event_title': event_title, 
            'event_date': event_date,    
            'check_in_time': check_in,
            'check_out_time': check_out,
            'attended': attended
        })
    return pd.DataFrame(rows)
