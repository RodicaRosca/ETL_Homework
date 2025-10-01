import oracledb
from sqlalchemy import create_engine, text
import pandas as pd
from parse_abs_exam import insert_exam_absences
from parse_trainings import extract_participants_df, parse_time
import extract_and_load_employees as emp_utils
import sample_with_month as abs_utils
from datetime import datetime

# ---------------- CONFIGURATION ----------------
DB_USERNAME = "hr"
DB_PASSWORD = "pass2025"
SOURCE_USERNAME = "source_schema"
SOURCE_PASSWORD = "source_schema2025"
TARGET_USERNAME = "target_schema"
TARGET_PASSWORD = "target_schema2025"
HOST = "127.0.0.1"
PORT = "1521"
SERVICE = "XEPDB1"

# ---------------- CONNECTION STRINGS ----------------
db_engine = create_engine(
    f"oracle+oracledb://{DB_USERNAME}:{DB_PASSWORD}"
    f"@{HOST}:{PORT}/?service_name={SERVICE}"
)

source_engine = create_engine(
    f"oracle+oracledb://{SOURCE_USERNAME}:{SOURCE_PASSWORD}"
    f"@{HOST}:{PORT}/?service_name={SERVICE}"
)

target_engine = create_engine(
    f"oracle+oracledb://{TARGET_USERNAME}:{TARGET_PASSWORD}"
    f"@{HOST}:{PORT}/?service_name={SERVICE}"
)

# ---------------- SOURCE SCHEMA DDL ----------------
source_ddl = """
CREATE TABLE absences_stage (
    employee_id NUMBER,
    absence_date DATE,
    absence_type VARCHAR2(30),
    reason VARCHAR2(100),
    duration NUMBER
);

CREATE TABLE training_attendance_stage (
    employee_id NUMBER,
    event_title VARCHAR2(100),
    event_date DATE,
    check_in_time TIMESTAMP,
    check_out_time TIMESTAMP,
    attended CHAR(1) CHECK (attended IN ('Y', 'N'))
);

CREATE TABLE employee_stage (
    employee_id NUMBER,
    full_name VARCHAR2(100),
    email VARCHAR2(100),
    buddy VARCHAR2(100),
    department VARCHAR2(100),
    role VARCHAR2(50),
    manager_name VARCHAR2(100)
);

CREATE TABLE project_stage (
    project_id VARCHAR2(50),
    name VARCHAR2(100),
    tech_stack VARCHAR2(100),
    manager_name VARCHAR2(100)
);
"""

# ---------------- TARGET SCHEMA DDL ----------------

target_ddl = """
-- 1. Absence Reason Dimension (type 1)
CREATE TABLE absence_reason_dim (
    reason_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reason_label VARCHAR2(50),
    is_paid CHAR(1) CHECK (is_paid IN ('Y', 'N')),
    is_partial_day CHAR(1) CHECK (is_partial_day IN ('Y', 'N'))
);

-- 2. Date Dimension (type 1)
CREATE TABLE date_dim (
    date_id NUMBER PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week VARCHAR2(10),
    week_number NUMBER,
    month VARCHAR2(15),
    year NUMBER,
    is_weekend CHAR(1),
    is_holiday CHAR(1)
);

-- 3. Employee Dimension (SCD2)
CREATE TABLE employee_dim (
    employee_sk NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    employee_id NUMBER NOT NULL,
    full_name VARCHAR2(100),
    buddy VARCHAR2(100),
    line_manager VARCHAR2(100),
    discipline VARCHAR2(100),
    delivery_unit VARCHAR2(100),
    grade VARCHAR2(100),
    record_start DATE NOT NULL,
    record_end DATE,
    is_current CHAR(1) DEFAULT 'Y' CHECK (is_current IN ('Y', 'N'))
);

-- 4. Event/Training Dimension (type 1)
CREATE TABLE event_dim (
    event_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type VARCHAR2(20),
    title VARCHAR2(100),
    trainer VARCHAR2(100),
    duration_minutes NUMBER,
    location VARCHAR2(100),
    date_id NUMBER
);

-- 5. Activity Type Dimension (type 1)
CREATE TABLE activity_type_dim (
    activity_type_sk NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    activity_type VARCHAR2(50)
);

-- 6. Fact Table
CREATE TABLE activity_fact (
    fact_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    employee_sk NUMBER NOT NULL,
    date_id NUMBER NOT NULL,
    activity_type_sk NUMBER NOT NULL,
    event_id NUMBER,
    absence_reason_id NUMBER,
    check_in_time TIMESTAMP,
    check_out_time TIMESTAMP,
    hours_worked NUMBER(4,2),
    attended CHAR(1),
    notes VARCHAR2(4000),
    CONSTRAINT fk_fact_employee FOREIGN KEY (employee_sk) REFERENCES employee_dim(employee_sk),
    CONSTRAINT fk_fact_date FOREIGN KEY (date_id) REFERENCES date_dim(date_id),
    CONSTRAINT fk_fact_activity_type FOREIGN KEY (activity_type_sk) REFERENCES activity_type_dim(activity_type_sk),
    CONSTRAINT fk_fact_event FOREIGN KEY (event_id) REFERENCES event_dim(event_id),
    CONSTRAINT fk_fact_absence_reason FOREIGN KEY (absence_reason_id) REFERENCES absence_reason_dim(reason_id)
);
"""

# ---------------- EXECUTE DDL ----------------
def execute_ddl(engine, ddl_script, schema_name):
    print(f"🛠 Creating tables in {schema_name}...")
    with engine.begin() as conn:
        for stmt in ddl_script.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))
    print(f"✅ {schema_name} setup complete.\n")

# Run for both schemas

execute_ddl(source_engine, source_ddl, "source_schema")
execute_ddl(target_engine, target_ddl, "target_schema")

# INSERT EMPLOYEE DATA

names_exam, emails_exam = emp_utils.extract_from_abs_exam('abs_exam.csv')
names_train, emails_train = set(), set()
for fname in ['trainings24.xlsx', 'trainings25.xlsx', 'trainings26.xlsx']:
    n, e = emp_utils.extract_from_trainings(fname)
    names_train |= n
    emails_train |= e

df_employees = emp_utils.build_employee_df([names_exam, names_train], [emails_exam, emails_train])

# Filter out duplicates before inserting
existing = pd.read_sql("SELECT employee_id, email FROM employees2", db_engine)
df_employees_filtered = df_employees[
    (~df_employees['employee_id'].isin(existing['employee_id'])) &
    (~df_employees['email'].isin(existing['email']))
]

emp_utils.insert_employees(df_employees_filtered, db_engine, table_name="employees2")
print(f"Inserted {len(df_employees_filtered)} new employees.")

# INSERT ABSENCE RECORDS FROM EXAMS

rows_to_insert = insert_exam_absences("abs_exam.csv")
print(rows_to_insert)

def calc_duration_hours(check_in, check_out):
    if pd.isnull(check_in) or pd.isnull(check_out):
        return None
    try:
        check_in = check_in.tz_localize(None)
        check_out = check_out.tz_localize(None)
    except AttributeError:
        pass
    delta = check_out - check_in
    return round(delta.total_seconds() / 3600, 2)

records = []
for row in rows_to_insert:
    absence_type = "exam" 
    reason = row.get('event_title')
    duration = calc_duration_hours(row['check_in_time'], row['check_out_time'])
    records.append({
        'employee_id': row.get('employee_id'),    
        'absence_date': row.get('event_date'),
        'absence_type': absence_type,
        'reason': reason,
        'duration': duration
    })

df_insert = pd.DataFrame(records)
df_insert['absence_date'] = pd.to_datetime(df_insert['absence_date'])
df_insert.to_sql('absences_stage', source_engine, if_exists='append', index=False)

# INSERT TRAINING ATTENDANCE RECORDS

df_attendance = extract_participants_df('trainings24.xlsx', 'employees2', db_engine)
df_attendance.to_sql('training_attendance_stage', source_engine, if_exists='append', index=False)
print("Inserted parsed attendance into training_attendance_stage!")

df_attendance = extract_participants_df('trainings25.xlsx', 'employees2', db_engine)
df_attendance.to_sql('training_attendance_stage', source_engine, if_exists='append', index=False)
print("Inserted parsed attendance into training_attendance_stage!")

df_attendance = extract_participants_df('trainings26.xlsx', 'employees2', db_engine)
df_attendance.to_sql('training_attendance_stage', source_engine, if_exists='append', index=False)
print("Inserted parsed attendance into training_attendance_stage!")

# INSERT ABSENCES FROM EXCEL 

df_absences = abs_utils.get_absence_rows_from_sample_xlsx('sample_absences_with_month.xlsx', db_engine)
print(df_absences.head()) 

df_absences.to_sql('absences_stage', source_engine, if_exists='append', index=False)
print(f"Inserted {len(df_absences)} absences.")

# INSERT EXAM ABSENCES

rows = insert_exam_absences('abs_exam.csv', db_engine)
df_to_insert = pd.DataFrame(rows)
print(df_to_insert.head())
df_to_insert.to_sql('absences_stage', source_engine, if_exists='append', index=False)

# INSERT TIMESHEET DATA

start_emp_id = 8
end_emp_id = 52
dates = ['2024-06-24', '2024-06-25', '2024-06-26']

with open('timesheet_inserts.sql', 'w') as f:
    for emp_id in range(start_emp_id, end_emp_id + 1):
        for date in dates:
            f.write(
                f"INSERT INTO timesheet (employee_id, work_date, hours_worked, check_in_time, check_out_time, notes) VALUES ({emp_id}, TO_DATE('{date}','YYYY-MM-DD'), 8.0, TO_TIMESTAMP('{date} 09:00:00','YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('{date} 17:00:00','YYYY-MM-DD HH24:MI:SS'), 'Worked on project PRJ1');\n"
            )

# 1. Populate date_dim

df_emp_dim = pd.read_sql("SELECT employee_sk, employee_id FROM employee_dim WHERE is_current='Y'", target_engine)
df_date_dim = pd.read_sql("SELECT date_id, full_date FROM date_dim", target_engine)
df_act_type = pd.read_sql("SELECT activity_type_sk, activity_type FROM activity_type_dim", target_engine)
df_reason_dim = pd.read_sql("SELECT reason_id, reason_label FROM absence_reason_dim", target_engine)

def generate_date_dim(start, end):
    dates = pd.date_range(start, end)
    df = pd.DataFrame({'full_date': dates})
    df['date_id'] = df['full_date'].dt.strftime('%Y%m%d').astype(int)
    df['day_of_week'] = df['full_date'].dt.day_name()
    df['week_number'] = df['full_date'].dt.isocalendar().week
    df['month'] = df['full_date'].dt.strftime('%B')
    df['year'] = df['full_date'].dt.year
    df['is_weekend'] = df['full_date'].dt.weekday >= 5
    df['is_weekend'] = df['is_weekend'].map({True: 'Y', False: 'N'})
    df['is_holiday'] = 'N'  
    return df[['date_id', 'full_date', 'day_of_week', 'week_number', 'month', 'year', 'is_weekend', 'is_holiday']]

date_dim = generate_date_dim('2025-01-01', '2025-12-30')
date_dim.to_sql('date_dim', target_engine, if_exists='append', index=False)

# 2. Populate absence_reason_dim

abs_stage = pd.read_sql('SELECT DISTINCT absence_type FROM absences_stage', source_engine)
reason_map = {
    'personal': ('personal', 'N', 'Y'),
    'facultate': ('facultate', 'N', 'Y'),
    'vacanta': ('vacanta', 'Y', 'N'),
    'exam': ('exam', 'N', 'N')
}
reason_rows = []
for t in abs_stage['absence_type'].unique():
    label, is_paid, is_partial = reason_map.get(str(t).lower(), (t, 'N', 'N'))
    reason_rows.append({'reason_label': label, 'is_paid': is_paid, 'is_partial_day': is_partial})
pd.DataFrame(reason_rows).drop_duplicates().to_sql('absence_reason_dim', target_engine, if_exists='append', index=False)

# Load absences from stage
absences = pd.read_sql('SELECT * FROM absences_stage', source_engine)

# Join to dimensions (including correct activity_type_sk!)
absence = (
    absences
    .merge(df_emp_dim, on='employee_id')
    .merge(df_date_dim, left_on='absence_date', right_on='full_date')
    .merge(df_act_type[df_act_type['activity_type'] == 'absence'], how='cross')
    .merge(df_reason_dim, left_on='absence_type', right_on='reason_label')
)

# Build absence fact table
absence_fact = absence[['employee_sk', 'date_id', 'activity_type_sk', 'reason_id', 'duration']].copy()
absence_fact['event_id'] = None
absence_fact['check_in_time'] = None
absence_fact['check_out_time'] = None
absence_fact['hours_worked'] = absence_fact['duration']
absence_fact['attended'] = None
absence_fact['notes'] = None
absence_fact = absence_fact.rename(columns={'reason_id': 'absence_reason_id'}).drop(columns=['duration'])

# Insert into fact table
absence_fact.to_sql('activity_fact', target_engine, if_exists='append', index=False)


#  3. Populate activity_type_dim

activity_types = ['work', 'absence', 'training']
pd.DataFrame({'activity_type': activity_types}).to_sql('activity_type_dim', target_engine, if_exists='append', index=False)

# 4. Populate event_dim (type 1)

event_stage = pd.read_sql('SELECT DISTINCT event_title, event_date FROM training_attendance_stage', source_engine)
event_stage['event_type'] = 'training'
event_stage['trainer'] = None  # You don't have this info, so set to None
event_stage['duration_minutes'] = 60  # Or your business logic
event_stage['location'] = None
event_stage['date_id'] = pd.to_datetime(event_stage['event_date']).dt.strftime('%Y%m%d').astype(int)
event_stage = event_stage.rename(columns={'event_title': 'title'})
event_stage[['event_type', 'title', 'trainer', 'duration_minutes', 'location', 'date_id']].to_sql(
    'event_dim', target_engine, if_exists='append', index=False
)

# 5. Populate employee_dim (SCD2, all as current)

employee_stage = pd.read_sql('SELECT * FROM employees2', db_engine)
employee_stage['record_start'] = pd.Timestamp.today()
employee_stage['record_end'] = None
employee_stage['is_current'] = 'Y'
employee_stage = employee_stage.rename(columns={
    'manager_id': 'line_manager',
    'department': 'discipline',
    'role': 'grade'
})
employee_stage[['employee_id', 'full_name', 'buddy', 'line_manager', 'discipline', 'delivery_unit', 'grade', 'record_start', 'record_end', 'is_current']].to_sql(
    'employee_dim', target_engine, if_exists='append', index=False
)

#--- Load dimension tables ---
df_emp_dim = pd.read_sql("SELECT employee_sk, employee_id FROM employee_dim WHERE is_current='Y'", target_engine)
df_date_dim = pd.read_sql("SELECT date_id, full_date FROM date_dim", target_engine)
df_act_type = pd.read_sql("SELECT activity_type_sk, activity_type FROM activity_type_dim", target_engine)
df_event_dim = pd.read_sql("SELECT event_id, title, date_id FROM event_dim", target_engine)

trainings = pd.read_sql("SELECT * FROM training_attendance_stage", source_engine)

trainings = trainings.dropna(subset=['employee_id'])

trainings['employee_id'] = trainings['employee_id'].astype(int)
df_emp_dim['employee_id'] = df_emp_dim['employee_id'].astype(int)

trainings['event_title_clean'] = trainings['event_title'].astype(str).str.strip().str.lower()
trainings['date_id'] = pd.to_datetime(trainings['event_date']).dt.strftime('%Y%m%d').astype(int)
df_event_dim['title_clean'] = df_event_dim['title'].astype(str).str.strip().str.lower()

step = trainings.merge(df_emp_dim, on='employee_id', how='inner')
print("Rows after employee merge:", len(step))

step = step.merge(df_date_dim, left_on='event_date', right_on='full_date', how='inner')
print("Rows after date merge:", len(step))

step = step.merge(df_act_type[df_act_type['activity_type'] == 'training'], how='cross')
print("Rows after activity type merge:", len(step))

if 'date_id' not in step.columns:
    step['date_id'] = pd.to_datetime(step['event_date']).dt.strftime('%Y%m%d').astype(int)

print("event_date sample from trainings:", trainings['event_date'].head(10))
print("full_date sample from df_date_dim:", df_date_dim['full_date'].head(10))
print("dtypes:", trainings['event_date'].dtype, df_date_dim['full_date'].dtype)

final = step.merge(df_event_dim, left_on=['event_title_clean', 'date_id'], right_on=['title_clean', 'date_id'], how='inner')
print("Rows after event merge:", len(final))

training_fact = final[['employee_sk', 'date_id', 'activity_type_sk', 'event_id', 'check_in_time', 'check_out_time', 'attended']].copy()
training_fact['absence_reason_id'] = None
training_fact['hours_worked'] = None
training_fact['notes'] = None

training_fact.to_sql('activity_fact', target_engine, if_exists='append', index=False)
print(f"Inserted {len(training_fact)} training attendance rows into activity_fact.")
