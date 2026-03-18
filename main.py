import io
import csv
import zipfile
import subprocess
import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# --- CONFIGURACIÓN ---
# IDs de tus carpetas de Drive
FOLDER_ID_ENTRADA = "1TB8fPJkli8-Qzke28VAugn2EyqQFGxmE"
FOLDER_ID_SALIDA = "1WK0HaCeEtTuOOPgJbT1mtGA-uJPAJTsQ"

# Encabezados oficiales
_HEADERS = {
    "BREB100": [
        "MOL_ID", "TRANSACTION_ID", "TRANSACTION_DATE", "TRANSACTION_MOL_DATE",
        "TRANSACTION_CREATION_DATE", "TRANSACTION_AMOUNT", "AUTHORIZER_PROVIDER",
        "NIT_AUTHORIZER_PROVIDER", "RECEIVER_PROVIDER", "NIT_RECEIVER_PROVIDER",
        "TRANSACTION_STATUS", "TRANSACTION_STATUS_CODE", "TRANSACTION_ERROR_CODE",
    ],
    "BREB101": [
        "TRANSACTION_DATE", "TRANSACTION_ID", "TRANSACTION_STATUS",
        "TRANSACTION_STATUS_CODE", "TRANSACTION_DETAIL", "TRANSACTION_TYPE",
        "TRANSACTION_AMOUNT",
    ]
}

def autenticar_drive():
    try:
        # Extrae el JSON de la cuenta de servicio desde el Secreto de GitHub
        creds_json = json.loads(os.environ.get('GCP_SA_KEY'))
        creds = service_account.Credentials.from_service_account_info(creds_json)
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        print(f"Error de autenticación (Revisa el Secreto GCP_SA_KEY): {e}")
        return None

def procesar_p7z_nuclear(contenido_p7z, nombre_p7z, service):
    """
    Lógica de Colab: OpenSSL -> Zip Interno -> TXT -> CSV -> Subida a Drive
    """
    try:
        # 1. Remoción de firma PKCS7 usando OpenSSL
        comando = ["openssl", "smime", "-verify", "-inform", "DER", "-noverify"]
        proceso = subprocess.run(comando, input=contenido_p7z, capture_output=True, check=True)
        datos_zip_interno = proceso.stdout
        
        # 2. Apertura de ZIP interno y extracción de TXT
        with zipfile.ZipFile(io.BytesIO(datos_zip_interno), 'r') as z_interno:
            nombre_txt = next((n for n in z_interno.namelist() if n.endswith('.txt')), None)
            if not nombre_txt:
                print(f"Error: No se encontró TXT dentro de {nombre_p7z}")
                return
            
            texto_plano = z_interno.read(nombre_txt).decode('utf-8')

        # 3. Transformación a CSV
        tipo = "BREB100" if "BREB100" in nombre_txt else "BREB101"
        output_csv = io.StringIO()
        writer = csv.DictWriter(output_csv, fieldnames=_HEADERS[tipo], delimiter=';', quoting=csv.QUOTE_ALL)
        writer.writeheader()
        
        reader = csv.DictReader(io.StringIO(texto_plano), fieldnames=_HEADERS[tipo], delimiter=';')
        for row in reader:
            writer.writerow(row)

        # 4. Subida directa a Google Drive
        nombre_final = nombre_txt.replace(".txt", ".csv")
        csv_bytes = io.BytesIO(output_csv.getvalue().encode('utf-8'))
        
        media = MediaIoBaseUpload(csv_bytes, mimetype='text/csv', resumable=True)
        metadata = {'name': f"PROCESADO_{nombre_final}", 'parents': [FOLDER_ID_SALIDA]}
        
        service.files().create(body=metadata, media_body=media, supportsAllDrives=True).execute()
        print(f"✅ Éxito: {nombre_p7z} procesado y subido como PROCESADO_{nombre_final}")

    except subprocess.CalledProcessError as e:
        print(f"❌ Error OpenSSL en {nombre_p7z}: {e.stderr.decode()}")
    except Exception as e:
        print(f"❌ Error general en {nombre_p7z}: {str(e)}")

def main():
    service = autenticar_drive()
    if not service: return

    # Listar archivos en la carpeta de entrada
    query = f"'{FOLDER_ID_ENTRADA}' in parents and trashed = false"
    results = service.files().list(
        q=query, 
        fields="files(id, name, mimeType)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    
    archivos = results.get('files', [])
    print(f"Archivos encontrados para procesar: {len(archivos)}")

    for file in archivos:
        if "PROCESADO" in file['name'] or file['mimeType'] == 'application/vnd.google-apps.folder':
            continue

        print(f"Leyendo archivo de Drive: {file['name']}")
        
        # Descarga el archivo de Drive a memoria
        request = service.files().get_media(fileId=file['id'])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        contenido = fh.getvalue()
        nombre_archivo = file['name'].lower()

        # Validador de entrada flexible (Lógica de tu Colab)
        if nombre_archivo.endswith('.zip'):
            with zipfile.ZipFile(io.BytesIO(contenido), 'r') as z_maestro:
                archivos_p7z = [f for f in z_maestro.namelist() if f.endswith('.p7z')]
                for p7z_interno in archivos_p7z:
                    procesar_p7z_nuclear(z_maestro.read(p7z_interno), p7z_interno, service)
        
        elif nombre_archivo.endswith('.p7z'):
            procesar_p7z_nuclear(contenido, file['name'], service)
        
        else:
            print(f"Saltando {file['name']}: No es .zip ni .p7z")

if __name__ == "__main__":
    main()
