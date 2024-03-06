"""
This module contains a script for running subprocesses.
"""
# scraping-data/scripts/yahoo_news/subprocess_script.py
import subprocess

SCRIPT_FILE1 = "./unzip_zip_file.py"
SCRIPT_FILE2 = "./scraping_from_yahoo_news.py"
SCRIPT_FILE3 = "./concat_only_unique_urls.py"
SCRIPT_FILE4 = "./remove_text_outliers.py"
SCRIPT_FILE5 = "./compress_csv_to_zip.py"

subprocess.run(["/home/linuxbrew/.linuxbrew/bin/python3.11", SCRIPT_FILE1], check=True)
subprocess.run(["/home/linuxbrew/.linuxbrew/bin/python3.11", SCRIPT_FILE2], check=True)
subprocess.run(["/home/linuxbrew/.linuxbrew/bin/python3.11", SCRIPT_FILE3], check=True)
subprocess.run(["/home/linuxbrew/.linuxbrew/bin/python3.11", SCRIPT_FILE4], check=True)
subprocess.run(["/home/linuxbrew/.linuxbrew/bin/python3.11", SCRIPT_FILE5], check=True)
