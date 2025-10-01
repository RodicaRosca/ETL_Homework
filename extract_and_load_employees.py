import pandas as pd
import numpy as np

def extract_from_abs_exam(file_path):
    df = pd.read_csv(file_path)
    names = []
    emails = []
    for idx, row in df.iterrows():
        names += [x.strip() for x in str(row.get('Attendees Names', '')).split(',') if x and str(row.get('Attendees Names', '')).lower() != 'nan']
        emails += [x.strip() for x in str(row.get('Attendees Emails', '')).split(',') if x and str(row.get('Attendees Emails', '')).lower() != 'nan']
    return set(names), set(emails)

def extract_from_absences_xlsx(file_path):
    df = pd.read_excel(file_path)
    if 'Employee' in df.columns:
        names = list(df['Employee'].dropna().unique())
    else:
        names = []
    return set(names)

def extract_from_trainings(file_path):
    names = set()
    emails = set()
    try:
        df = pd.read_excel(file_path, header=9)
        for _, row in df.iterrows():
            n = row.get('Name')
            e = row.get('Email')
            if pd.notnull(n):
                names.add(str(n).strip())
            if pd.notnull(e):
                emails.add(str(e).strip())
    except Exception as e:
        print(f"Could not read {file_path}: {e}")
    return names, emails

def build_employee_df(name_sets, email_sets):
    all_names = set()
    all_emails = set()
    for s in name_sets:
        all_names |= s
    for s in email_sets:
        all_emails |= s

    employees = []
    for name in all_names:
        matched_email = None
        for email in all_emails:
            name_parts = name.lower().split()
            if all(p in email.lower() for p in name_parts if len(p) > 2):
                matched_email = email
                break
        employees.append({"full_name": name, "email": matched_email})

    df_employees = pd.DataFrame(employees)
    df_employees = df_employees.drop_duplicates(subset=['full_name', 'email'])
    df_employees['employee_id'] = np.arange(1, len(df_employees) + 1)
    df_employees['buddy'] = "Popescu Ion"
    df_employees['department'] = "Data & AI"
    df_employees['role'] = "Junior Data Engineer"
    df_employees['manager_id'] = 1
    df_employees['delivery_unit'] = "IS"

    return df_employees[['employee_id', 'full_name', 'email', 'buddy', 'department', 'role', 'manager_id', 'delivery_unit']]

def insert_employees(df_employees, engine, table_name="employees2"):
    df_employees.to_sql(table_name, engine, if_exists="append", index=False)
