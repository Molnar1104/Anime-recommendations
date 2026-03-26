-- Create separate Airflow database and raw schema for anime data
CREATE DATABASE airflow_db;

\connect anime_db;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
