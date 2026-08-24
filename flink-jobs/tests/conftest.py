"""
Conftest: add flink-jobs/ to sys.path so tests can import from common/ and job files.
"""
import sys
import os

# flink-jobs/ directory
FLINK_JOBS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if FLINK_JOBS_DIR not in sys.path:
    sys.path.insert(0, FLINK_JOBS_DIR)
