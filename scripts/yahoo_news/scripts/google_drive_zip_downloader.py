# text_classification/data/scraping_data/scripts/google_drive_zip_downloader.py
import io
from googleapiclient.http import MediaIoBaseDownload

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

def download_zip_files(service, folder_id, local_folder_path):
    query = f"'{folder_id}' in parents and mimeType='application/zip'"
    results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    items = results.get('files', [])

    if not items:
        print("No ZIP files found in the folder.")
        return

    for item in items:
        file_id = item['id']
        file_name = item['name']
        local_file_path = os.path.join(local_folder_path, file_name)
        print(f"Downloading {file_name}...")

        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}%.")

        with open(local_file_path, 'wb') as f:
            fh.seek(0)
            f.write(fh.read())

        print(f"Downloaded {file_name} to {local_file_path}")

def main():
    service = authenticate_google_drive()
    local_folder_path = '../csv/yahoo_news/concat'
    download_zip_files(service, drive_folder_id, local_folder_path)

if __name__ == '__main__':
    main()
