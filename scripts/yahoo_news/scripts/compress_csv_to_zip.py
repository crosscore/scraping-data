import os
import zipfile

directory = '../csv/yahoo_news/concat/'

# List all files in the directory
files = os.listdir(directory)

# Find the first CSV file
csv_file = next((f for f in files if f.endswith('.csv')), None)

if csv_file:
    # Full path for the CSV file
    csv_path = os.path.join(directory, csv_file)

    # Full path for the ZIP file (same name as the CSV but with .zip extension)
    zip_path = os.path.splitext(csv_path)[0] + '.zip'

    # Creating a ZIP file
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(csv_path, csv_file)

    # Remove the original CSV file
    os.remove(csv_path)

    # Print complete messages
    print(f'CSV file {csv_file} compressed and removed successfully.')
    print(f'ZIP file {zip_path} created successfully.')
else:
    print("No CSV file found in the directory.")
