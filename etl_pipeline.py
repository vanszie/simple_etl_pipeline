import pandas as pd
import os
from datetime import datetime

# Define base paths
base_dir = "data"
source_dir = f"{base_dir}/source"
staging_dir = f"{base_dir}/staging"
dwh_dir = f"{base_dir}/dwh"
mart_dir = f"{base_dir}/mart"
os.makedirs(staging_dir, exist_ok=True)
os.makedirs(dwh_dir, exist_ok=True)
os.makedirs(mart_dir, exist_ok=True)

# Function to get file creation datetime
def get_file_creation_datetime(file_path):
    ts = os.path.getctime(file_path)
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

# Source file paths
course_path = f"{source_dir}/course.csv"
schedule_path = f"{source_dir}/schedule.csv"
enrollment_path = f"{source_dir}/enrollment.csv"
attendance_path = f"{source_dir}/course_attendance.csv"

# Load CSV files
course_df = pd.read_csv(f"{source_dir}/course.csv")
schedule_df = pd.read_csv(f"{source_dir}/schedule.csv")
enrollment_df = pd.read_csv(f"{source_dir}/enrollment.csv")
course_attendance_df = pd.read_csv(f"{source_dir}/course_attendance.csv")

# --- RENAME ID BASE ON FILE NAME ---
course_df.rename(columns={"ID": "COURSE_ID", "NAME": "COURSE_NAME"}, inplace=True)
schedule_df.rename(columns={"ID": "SCHEDULE_ID"}, inplace=True)
enrollment_df.rename(columns={"ID": "ENROLLMENT_ID", "SEMESTER": "SEMESTER_ID"}, inplace=True)
course_attendance_df.rename(columns={"ID": "COURSE_ATTENDANCE_ID"}, inplace=True)

# --- Add file_creation_datetime from source file creation datetime in staging---
course_df["file_creation_datetime"] = get_file_creation_datetime(course_path)
schedule_df["file_creation_datetime"] = get_file_creation_datetime(schedule_path)
enrollment_df["file_creation_datetime"] = get_file_creation_datetime(enrollment_path)
course_attendance_df["file_creation_datetime"] = get_file_creation_datetime(attendance_path)

# --- Add Load datetime to staging--

load_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
course_df["load_datetime"] = load_dt
schedule_df["load_datetime"] = load_dt
enrollment_df["load_datetime"] = load_dt
course_attendance_df["load_datetime"] = load_dt

# Save staging data
course_df.to_csv(f"{staging_dir}/course.csv", index=False)
schedule_df.to_csv(f"{staging_dir}/schedule.csv", index=False)
enrollment_df.to_csv(f"{staging_dir}/enrollment.csv", index=False)
course_attendance_df.to_csv(f"{staging_dir}/course_attendance.csv", index=False)

# --- JOIN data course with schedule  ---
course_schedule = schedule_df.merge(course_df, on="COURSE_ID", how="left")

# --- JOIN data enrollment with course_schedule ---
enrollment_schedule = enrollment_df.merge(course_schedule, on="SCHEDULE_ID", how="left")
enrollment_course_schedule = (
    enrollment_schedule
    .groupby(["SEMESTER_ID", "COURSE_NAME"])
    .agg(student_count=("STUDENT_ID", "nunique"))
    .reset_index()
)

# --- JOIN  data course_attendance with course_schedule---
course_attendance_df["ATTEND_DT"] = pd.to_datetime(course_attendance_df["ATTEND_DT"])
course_schedule["START_DT"] = pd.to_datetime(course_schedule["START_DT"])
course_schedule["END_DT"] = pd.to_datetime(course_schedule["END_DT"])

join_data = course_attendance_df.merge(course_schedule, on="SCHEDULE_ID", how="left")
join_data = join_data.merge(enrollment_df[["ENROLLMENT_ID","STUDENT_ID", "SCHEDULE_ID", "ACADEMIC_YEAR", "SEMESTER_ID", "ENROLL_DT"]],
                            on=["STUDENT_ID", "SCHEDULE_ID"], how="left")

join_data["WEEK_ID"] = (
    ((join_data["END_DT"] - join_data["START_DT"]).dt.days // 7)
    - ((join_data["END_DT"] - join_data["ATTEND_DT"]).dt.days // 7) + 1
)
join_data = join_data[join_data["WEEK_ID"].between(1, 13)]

# --- Prepare fact_course_attendance_detail.csv ---
fact_df = join_data[[
    "COURSE_ATTENDANCE_ID",
    "ENROLLMENT_ID",
    "STUDENT_ID",
    "COURSE_NAME",
    "SEMESTER_ID",
    "WEEK_ID",
    "LECTURER_ID",
    "START_DT",
    "END_DT",
    "ATTEND_DT",
    "ACADEMIC_YEAR",
    "ENROLL_DT"
]].copy()


# Save to CSV
fact_df.to_csv(f"{dwh_dir}/fact_course_attendance_detail.csv", index=False)

# --- Calculate Course Attendance ---
attendance_summary = (
    fact_df.groupby(["SEMESTER_ID", "WEEK_ID", "COURSE_NAME"])
    .agg(ATT_COUNT=("STUDENT_ID", "nunique"))
    .reset_index()
)

attendance_summary = attendance_summary.merge(
    enrollment_course_schedule,
    left_on=["SEMESTER_ID", "COURSE_NAME"],
    right_on=["SEMESTER_ID", "COURSE_NAME"],
    how="left"
)

attendance_summary["ATTENDANCE_PCT"] = round(
    (attendance_summary["ATT_COUNT"] / attendance_summary["student_count"]).clip(upper=1) * 100, 2
)

# --- Final formatting ---
attendance_summary = attendance_summary[["SEMESTER_ID", "WEEK_ID", "COURSE_NAME", "ATTENDANCE_PCT"]]

# --- Export to CSV ---
output_path = f"{mart_dir}/weekly_course_attendance.csv"
attendance_summary.to_csv(output_path, index=False)

print("Weekly attendance report generated.")
print(f"Saved to: {output_path}")
