# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import current_timestamp

# Urls from the spreedsheets

url_schools="https://docs.google.com/spreadsheets/d/e/2PACX-1vQcLrKPO9MWkymnrtjZpGrN73gMM5duSQsPJHT6vts_yt8f4NmAU3KJndDV0tAt9jFQLVNF-4WGztZL/pub?gid=984221100&single=true&output=csv"
url_teachers="https://docs.google.com/spreadsheets/d/e/2PACX-1vQcLrKPO9MWkymnrtjZpGrN73gMM5duSQsPJHT6vts_yt8f4NmAU3KJndDV0tAt9jFQLVNF-4WGztZL/pub?gid=1925280015&single=true&output=csv"
url_workshops="https://docs.google.com/spreadsheets/d/e/2PACX-1vQcLrKPO9MWkymnrtjZpGrN73gMM5duSQsPJHT6vts_yt8f4NmAU3KJndDV0tAt9jFQLVNF-4WGztZL/pub?gid=53529070&single=true&output=csv"
url_students="https://docs.google.com/spreadsheets/d/e/2PACX-1vQcLrKPO9MWkymnrtjZpGrN73gMM5duSQsPJHT6vts_yt8f4NmAU3KJndDV0tAt9jFQLVNF-4WGztZL/pub?gid=0&single=true&output=csv"
url_attendance="https://docs.google.com/spreadsheets/d/e/2PACX-1vQcLrKPO9MWkymnrtjZpGrN73gMM5duSQsPJHT6vts_yt8f4NmAU3KJndDV0tAt9jFQLVNF-4WGztZL/pub?gid=2139114709&single=true&output=csv"

# Paths to tables
school_table_path = " education_system.operation.schools"
teacher_table_path = " education_system.operation.teachers"
workshop_table_path = " education_system.operation.workshops"
student_table_path = " education_system.operation.students"
attendance_table_path = " education_system.operation.attendance"

#Read with pandas

df_schools = pd.read_csv(url_schools)
df_teachers = pd.read_csv(url_teachers)
df_workshops = pd.read_csv(url_workshops)
df_students = pd.read_csv(url_students)
df_attendance = pd.read_csv(url_attendance)


#Read with spark

spark_df_schools = spark.createDataFrame(df_schools)
spark_df_teachers = spark.createDataFrame(df_teachers)
spark_df_workshops = spark.createDataFrame(df_workshops)
spark_df_students = spark.createDataFrame(df_students)
spark_df_attendance = spark.createDataFrame(df_attendance)


# Add timestamp
schools=spark_df_schools.withColumn("timestamp", current_timestamp())
teachers=spark_df_teachers.withColumn("timestamp", current_timestamp())
workshops=spark_df_workshops.withColumn("timestamp", current_timestamp())
students=spark_df_students.withColumn("timestamp", current_timestamp())
attendance=spark_df_attendance.withColumn("timestamp", current_timestamp())


# Write (Explicit Delta + Append)

(schools.write
    .format("delta")       # Explicit is better
    .mode("append")        # Append allows DLT streaming to work without crashing
    .option("mergeSchema", "true") # Auto-fix if you add a new column in Google Sheets
    .saveAsTable(school_table_path)
)

(teachers.write
    .format("delta")       # Explicit is better
    .mode("append")        # Append allows DLT streaming to work without crashing
    .option("mergeSchema", "true") # Auto-fix if you add a new column in Google Sheets
    .saveAsTable(teacher_table_path)
)

(workshops.write
    .format("delta")       # Explicit is better
    .mode("append")        # Append allows DLT streaming to work without crashing
    .option("mergeSchema", "true") # Auto-fix if you add a new column in Google Sheets
    .saveAsTable(workshop_table_path)
)

(students.write
    .format("delta")       # Explicit is better
    .mode("append")        # Append allows DLT streaming to work without crashing
    .option("mergeSchema", "true") # Auto-fix if you add a new column in Google Sheets
    .saveAsTable(student_table_path)
)

(attendance.write
 .format("delta")       # Explicit is better
 .mode("append")        # Append allows DLT streaming to work without crashing
 .option("mergeSchema", "true") # Auto-fix if you add a new column in Google Sheets
 .saveAsTable(attendance_table_path)
)


# Displaying tables

display(spark.table(school_table_path))
display(spark.table(teacher_table_path))
display(spark.table(workshop_table_path))
display(spark.table(student_table_path))
display(spark.table(attendance_table_path))