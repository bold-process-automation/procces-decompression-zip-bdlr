import io
import csv
import zipfile
import subprocess
import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# CONFIGURACIÓN
FOLDER_ID_SALIDA = "1WK0HaCeEtTuOOPgJbT1mtGA-uJPAJTsQ"

_HEADERS = {
    "BREB100": ["MOL_ID", "TRANSACTION_ID", "TRANSACTION_DATE", "TRANSACTION_MOL_DATE", "TRANSACTION_CREATION_DATE", "TRANSACTION_AMOUNT", "AUTHORIZER_PROVIDER", "NIT_AUTHORIZER_PROVIDER", "RECEIVER_PROVIDER", "NIT_RECEIVER_PROVIDER", "TRANSACTION_STATUS", "TRANSACTION_STATUS_CODE", "TRANSACTION_ERROR_CODE"],
    "BREB101": ["TRANSACTION_DATE", "TRANSACTION_ID", "TRANSACTION_STATUS", "TRANSACTION_STATUS_CODE", "TRANSACTION_DETAIL", "TRANSACTION_TYPE", "TRANSACTION_AMOUNT"]
}

def autenticar_drive():
    try:
        creds_json = json.loads(os.environ.get('GCP_SA_KEY'))
        creds = service_account.Credentials.from_service_account_info(creds_json)
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        print(f"Error autenticación: {e}")
        return None

def descargar_de_drive(service, file_id):
    """Descarga el archivo desde Drive usando el ID enviado por n8n."""
    try:
        file_metadata = service.files().get(fileId=file_id).execute()
        nombre = file_metadata['name']
        
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        return fh.getvalue(), nombre
    except Exception as e:
        print(f"Error descargando de Drive: {e}")
        return None, None

def procesar_p7z(contenido_p7z, nombre_p7z, service):
    """Desencripta y transforma el archivo."""
    try:
        proceso = subprocess.run(
            ["openssl", "smime", "-verify", "-inform", "DER", "-noverify"],
            input=contenido_p7z, capture_output=True, check=True
        )
        with zipfile.ZipFile(io.BytesIO(proceso.stdout), 'r') as z:
            nombre_txt = next((n for n in z.namelist() if n.endswith('.txt')), None)
            texto = z.read(nombre_txt).decode('utf-8')

        tipo = "BREB100" if "BREB100" in nombre_txt else "BREB101"
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=_HEADERS[tipo], delimiter=';', quoting=csv.QUOTE_ALL)
        writer.writeheader()
        reader = csv.DictReader(io.StringIO(texto), fieldnames=_HEADERS[tipo], delimiter=';')
        for row in reader: writer.writerow(row)

        # Subir resultado a Drive
        nombre_csv = nombre_txt.replace(".txt", ".csv")
        media = MediaIoBaseUpload(io.BytesIO(output.getvalue().encode('utf-8')), mimetype='text/csv')
        service.files().create(body={'name': nombre_csv, 'parents': [FOLDER_ID_SALIDA]}, media_body=media).execute()
        print(f"Éxito: {nombre_csv} procesado.")
    except Exception as e:
        print(f"Fallo en {nombre_p7z}: {e}")

if __name__ == "__main__":
    file_id = os.environ.get('ID_ARCHIVO_DRIVE')
    if not file_id:
        print("Error: No se recibió ID_ARCHIVO_DRIVE.")
    else:
        service = autenticar_drive()
        contenido, nombre = descargar_de_drive(service, file_id)
        if contenido:
            if nombre.lower().endswith('.zip'):
                with zipfile.ZipFile(io.BytesIO(contenido), 'r') as z:
                    for p7z in [f for f in z.namelist() if f.endswith('.p7z')]:
                        procesar_p7z(z.read(p7z), p7z, service)
            else:
                procesar_p7z(contenido, nombre, service)
