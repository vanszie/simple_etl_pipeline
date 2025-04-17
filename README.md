# 🎓 Simple ETL Pipeline - Weekly Course Attendance Report

This project is an ETL pipeline built in Python using pandas. It processes university course data to generate a weekly course attendance report (in %) by semester and week.

---

## 📊 Report Output

The final report is saved as a CSV file:

`data/mart/weekly_course_attendance.csv`

### Sample Output Format:

| SEMESTER_ID | WEEK_ID | COURSE_NAME        | ATTENDANCE_PCT |
|-------------|---------|--------------------|----------------|
| 1           | 1       | Calculus I         | 94.12          |
| 1           | 2       | Introduction to AI | 88.89          |

---

## 📁 Folder Structure

. ├── Dockerfile 
  ├── etl_pipeline.py 
  ├── README.md 
  └── data/ 
    ├── source/ │ 
        ├── course.csv │ 
        ├── schedule.csv │ 
        ├── enrollment.csv │ 
        └── course_attendance.csv 
    ├── staging/ # Generated during ETL run
    ├── dwh/ # Generated during ETL run 
    └── mart/ # Contains final CSV output

## 🐳 Running with Docker

### 1. Build Docker Image

docker build -t etl-pipeline .

### 2. Run Docker Container
Mount the local data/ directory so that you can access the output:

docker run --rm -v "$PWD/data":/app/data etl-pipeline

## ⚙️ What It Does

### Source Layer
Extract all csv file into pandas datafreame

### Staging Layer
Renames columns, parses datetimes, and adds a file_creation_datetime and load_datetime columns to all staging CSVs.

### dwh Layer
Joins the course, schedule, enrollment, and attendance data. 
save join_data to file fact_course_attendance_detail.csv

### Mart Layer
Calculates & summarize weekly attendance per course by semester.
Outputs a clean CSV report: weekly_course_attendance.csv

## ⬇️ Requirements (for local run)
If not using Docker:

pip install pandas
python etl_pipeline.py