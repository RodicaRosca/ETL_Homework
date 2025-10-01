# import pandas as pd

# df = pd.read_excel('sample_absences_with_month.xlsx')

# code_to_type = {'P': 'personal', 'F': 'facultate', 'V': 'vacanta'}

# def extract_absence_info(cell):
#     if not isinstance(cell, str) or not cell.strip():
#         return None, None
#     parts = cell.strip().split()
#     if len(parts) != 2:
#         return None, None
#     code, duration = parts
#     absence_type = code_to_type.get(code.upper(), 'unknown')
#     try:
#         duration_val = float(duration) if '.' in duration else int(duration)
#     except ValueError:
#         duration_val = None
#     return absence_type, duration_val

# # Iterate through each row (employee)
# for idx, row in df.iterrows():
#     month = row['Month']
#     employee = row['Employee']
#     # Iterate through each day column
#     for day in df.columns[2:]:
#         absence_type, duration = extract_absence_info(row[day])
#         if absence_type and duration:
#             print(f"Month: {month}, Employee: {employee}, Day: {day}, Absence type: {absence_type}, Duration: {duration}")

import pandas as pd

def get_absence_rows_from_sample_xlsx(file_path, engine, employees_table="employees2"):
    df = pd.read_excel(file_path)
    df_emps = pd.read_sql(f"SELECT employee_id, full_name FROM {employees_table}", engine)
    name_to_id = dict(zip(df_emps['full_name'], df_emps['employee_id']))

    code_to_type = {'P': 'personal', 'F': 'facultate', 'V': 'vacanta'}

    def extract_absence_info(cell):
        if not isinstance(cell, str) or not cell.strip():
            return None, None
        parts = cell.strip().split()
        if len(parts) != 2:
            return None, None
        code, duration = parts
        absence_type = code_to_type.get(code.upper(), 'unknown')
        try:
            duration_val = float(duration) if '.' in duration else int(duration)
        except ValueError:
            duration_val = None
        return absence_type, duration_val

    def get_absence_date(month, day_label):
        try:
            base = pd.to_datetime(month, format='%Y-%m')
            day_num = int(day_label.split()[-1])
            return base.replace(day=day_num)
        except Exception:
            return None

    rows = []
    for idx, row in df.iterrows():
        month = row['Month']
        employee = row['Employee']
        employee_id = name_to_id.get(employee)
        if not employee_id:
            continue
        for day in df.columns[2:]:
            absence_type, duration = extract_absence_info(row[day])
            if absence_type and duration:
                absence_date = get_absence_date(month, day)
                rows.append({
                    'employee_id': employee_id,
                    'absence_date': absence_date,
                    'absence_type': absence_type,
                    'reason': None,
                    'duration': duration
                })

    return pd.DataFrame(rows)
