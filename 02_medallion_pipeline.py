# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# ==========================================
# BRONZE LAYER (Reading the Landing Zone)
# ==========================================

@dlt.table(comment="Raw schools history")
def bronze_schools():
    return spark.readStream.table("education_system.operation.schools")

@dlt.table(comment="Raw teachers history")
def bronze_teachers():
    return spark.readStream.table("education_system.operation.teachers")

@dlt.table(comment="Raw workshops history")
def bronze_workshops():
    return spark.readStream.table("education_system.operation.workshops")

@dlt.table(comment="Raw students history")
def bronze_students():
    return spark.readStream.table("education_system.operation.students")

@dlt.table(comment="Raw attendance history")
def bronze_attendance():
    return spark.readStream.table("education_system.operation.attendance")

# ======================================================
# SILVER LAYER (Processing & Deduplication)
# ======================================================

# ------------------------------------------------------
# 1. SCHOOLS
# ------------------------------------------------------
# Step A: Clean (Apply Expectations Here)
@dlt.table(name="silver_schools_clean", comment="Cleaning schools data")
@dlt.expect_or_drop("valid_id", "school_id IS NOT NULL")
def silver_schools_clean():
    return dlt.read_stream("bronze_schools")

# Step B: Deduplicate (Write to Final Table)
dlt.create_streaming_table("silver_schools")

dlt.apply_changes(
    target = "silver_schools",
    source = "silver_schools_clean",
    keys = ["school_id"], 
    sequence_by = col("timestamp"), 
    stored_as_scd_type = 1
)

# ------------------------------------------------------
# 2. TEACHERS
# ------------------------------------------------------
@dlt.table(name="silver_teachers_clean", comment="Cleaning teachers data")
@dlt.expect("valid_email", "email LIKE '%@%'")
def silver_teachers_clean():
    return dlt.read_stream("bronze_teachers")

dlt.create_streaming_table("silver_teachers")

dlt.apply_changes(
    target = "silver_teachers",
    source = "silver_teachers_clean",
    keys = ["teacher_id"],
    sequence_by = col("timestamp"),
    stored_as_scd_type = 1
)

# ------------------------------------------------------
# 3. WORKSHOPS
# ------------------------------------------------------
@dlt.table(name="silver_workshops_clean", comment="Cleaning workshops data")
@dlt.expect("positive_duration", "duration_hours > 0")
def silver_workshops_clean():
    return dlt.read_stream("bronze_workshops")

dlt.create_streaming_table("silver_workshops")

dlt.apply_changes(
    target = "silver_workshops",
    source = "silver_workshops_clean",
    keys = ["workshop_id"],
    sequence_by = col("timestamp"),
    stored_as_scd_type = 1
)

# ------------------------------------------------------
# 4. STUDENTS
# ------------------------------------------------------
@dlt.table(name="silver_students_clean", comment="Cleaning students data")
@dlt.expect("valid_grade", "grade IS NOT NULL")
def silver_students_clean():
    return dlt.read_stream("bronze_students")

dlt.create_streaming_table("silver_students")

dlt.apply_changes(
    target = "silver_students",
    source = "silver_students_clean",
    keys = ["student_id"],
    sequence_by = col("timestamp"),
    stored_as_scd_type = 1
)

# ------------------------------------------------------
# 5. ATTENDANCE
# ------------------------------------------------------
@dlt.table(name="silver_attendance_clean", comment="Cleaning attendance data")
@dlt.expect("valid_status", "status IN ('Present', 'Absent', 'Excused')")
def silver_attendance_clean():
    return (
        dlt.read_stream("bronze_attendance")
        .select(
            col("attendance_id"),
            col("student_id"),
            col("workshop_id"),
            to_date(col("date"), "yyyy-MM-dd").alias("class_date"), 
            col("status"),
            col("timestamp") 
        )
    )

dlt.create_streaming_table("silver_attendance")

dlt.apply_changes(
    target = "silver_attendance",
    source = "silver_attendance_clean",
    keys = ["attendance_id"],
    sequence_by = col("timestamp"),
    stored_as_scd_type = 1
)

# ==========================================
# GOLD LAYER (The Star Schema)
# ==========================================

@dlt.table(comment="Fact Table: Full Attendance details with all names joined")
def gold_fact_attendance():
    att = dlt.read("silver_attendance")
    stu = dlt.read("silver_students")
    wrk = dlt.read("silver_workshops")
    tch = dlt.read("silver_teachers")
    sch = dlt.read("silver_schools")

    return (
        att
        .join(stu, ["student_id"], "left")
        .join(wrk, ["workshop_id"], "left")
        .join(tch, wrk.teacher_id == tch.teacher_id, "left")
        .join(sch, stu.school_id == sch.school_id, "left")
        .select(
            att.attendance_id,
            att.class_date,
            att.status,
            stu.first_name.alias("student_name"),
            stu.grade,
            sch.school_name,
            wrk.workshop_title,
            tch.last_name.alias("teacher_lastname")
        )
    )