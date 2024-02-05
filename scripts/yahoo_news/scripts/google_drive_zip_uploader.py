# text_classification/data/scraping_data/scripts/google_drive_zip_uploader.py
import os
import glob
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

SERVICE_ACCOUNT_FOLDER = '../json/'
SCOPES = ['https://www.googleapis.com/auth/drive']
drive_folder_id = os.environ['GOOGLE_DRIVE_YAHOO_NEWS_FOLDER_ID']

def authenticate_google_drive():
    credentials_files = glob.glob(os.path.join(SERVICE_ACCOUNT_FOLDER, '*.json'))
    print(f"Service account files found: {credentials_files}")
    if not credentials_files:
        raise Exception("No service account file found.")
    creds = service_account.Credentials.from_service_account_file(credentials_files[0], scopes=SCOPES)
    return build('drive', 'v3', credentials=creds)

def delete_all_files_in_folder(service, folder_id):
    results = service.files().list(q=f"'{folder_id}' in parents", spaces='drive', fields='files(id, name)').execute()
    items = results.get('files', [])
    if items:
        for item in items:
            print(f"Deleting file: {item['name']} (ID: {item['id']})")
            service.files().delete(fileId=item['id']).execute()

def upload_files(service, folder_path, drive_folder_id):
    delete_all_files_in_folder(service, drive_folder_id)
    zip_files = glob.glob(os.path.join(folder_path, '*.zip'))
    if not zip_files:
        print("No ZIP files found to upload.")
        return
    for file_path in zip_files:
        print(f"Uploading {file_path}...")
        file_metadata = {'name': os.path.basename(file_path), 'parents': [drive_folder_id]}
        media = MediaFileUpload(file_path, mimetype='application/zip')
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"Uploaded file ID: {file.get('id')}")

def main():
    service = authenticate_google_drive()
    local_folder_path = '../csv/yahoo_news/concat'
    upload_files(service, local_folder_path, drive_folder_id)

if __name__ == '__main__':
    main()
