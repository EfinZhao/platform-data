# platform-data

A repository for logging and analyzing data from the UCI Public Road Network Platform.


# logging

logger_rt.py logs real-time API calls to data/rt/

run_rt.py batch runs the logger_rt.py script for each AI-System in udid.csv

logger_core_safety.py logs core and safety metric API calls to data/core_safety/

latency.py logs timestamps for real-time API data leaving edge boxes and arriving at local computer


# processing


# analysis

latency_analysis.py performs statistical analysis of latency between data leaving edge box (timestamp on Ouster API calls) and arriving at computer (can force process logged data with --reprocess flag, otherwise latency_processed.csv is used)

metrics.py returns high-level metrics from core and safety API calls

core_safety_loader.py supports core and safety API call data analysis in metrics.py


# lib

lib holds the Ouster API .whl files for various versions


# Data stored externally
core_safety are logs from logger_core_safety.py
core_safety_processed are processed logs from logger_core_safety.py
latency_logs look at the latency between edge box departure and local receipt of real-time API calls


# Miscellaneous

udid.csv is a list of AI-System installations with associated metadata.
intersection_viewer.py subscribes to real-time API calls and prints data to terminal for inspection
