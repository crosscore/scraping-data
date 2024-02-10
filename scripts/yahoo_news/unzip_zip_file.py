""" Unzip the first ZIP file in the directory and remove it. """
import os
import zipfile

DIRECTORY = '../../data/csv/yahoo_news/concat/'

# List all files in the directory
files = os.listdir(DIRECTORY)

# Find the first ZIP file
zip_file = next((f for f in files if f.endswith('.zip')), None)

if zip_file:
    # Full path for the ZIP file
    zip_path = os.path.join(DIRECTORY, zip_file)

    # Extracting the ZIP file
    with zipfile.ZipFile(zip_path, 'r') as zipf:
        zipf.extractall(DIRECTORY)

    # Remove the original ZIP file
    os.remove(zip_path)

    print(f'ZIP file {zip_file} extracted and removed successfully.')
else:
    print("No ZIP file found in the directory.")
