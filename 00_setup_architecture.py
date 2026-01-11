# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- Project data structure hierarchy
# MAGIC
# MAGIC --1. Create the catalog
# MAGIC CREATE CATALOG IF NOT EXISTS education_system;
# MAGIC
# MAGIC -- 2. Create the Schema 
# MAGIC CREATE SCHEMA IF NOT EXISTS education_system.operation;
# MAGIC
# MAGIC -- 3. Verify it worked
# MAGIC DESCRIBE SCHEMA education_system.operation;