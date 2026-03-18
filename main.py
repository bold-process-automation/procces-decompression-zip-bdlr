import io
import csv
import zipfile
import subprocess
import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# --- CONFIGURACIÓN DE CARPETAS ---
FOLDER_ID_ENTRADA = "1TB8fPJkli8-Qzke28VAugn2EyqQFGxmE" # Donde n8n deja los archivos
FOLDER_ID_SALIDA = "1WK0HaCeEtTuOOPgJbT1mtGA-uJPAJTsQ"  # Donde quieres los CSVs

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

def procesar_p7z(contenido_p7z, nombre_original, service):
    """Desencripta con OpenSSL y transforma el contenido a CSV."""
    try:
        # 1. Desencriptar usando OpenSSL
        proceso = subprocess.run(
            ["openssl", "smime", "-verify", "-inform", "DER", "-noverify"],
            input=contenido_p7z, capture_output=True, check=True
        )
        
        # 2. Abrir el ZIP resultante de la desencriptación
        with zipfile.ZipFile(io.BytesIO(proceso.stdout), 'r') as z:
            nombre_txt = next((n for n in z.namelist() if n.endswith('.txt')), None)
            if not nombre_txt:
                print(f"No se encontró archivo .txt dentro de {nombre_original}")
                return
            
            texto = z.read(nombre_txt).decode('utf-8')

        # 3. Identificar tipo y transformar a CSV
        tipo = "BREB100" if "BREB100" in nombre_txt else "BREB101"
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=_HEADERS[tipo], delimiter=';', quoting=csv.QUOTE_ALL)
        writer.writeheader()
        
        reader = csv.DictReader(io.StringIO(texto), fieldnames=_HEADERS[tipo], delimiter=';')
        for row in reader: 
            writer.writerow(row)

        # 4. Subir resultado a Drive
        nombre_csv = nombre_txt.replace(".txt", ".csv")
        media = MediaIoBaseUpload(
            io.BytesIO(output.getvalue().encode('utf-8')), 
            mimetype='text/csv'
        )
        
        service.files().create(
            body={'name': f"PROCESADO_{nombre_csv}", 'parents': [FOLDER_ID_SALIDA]}, 
            media_body=media,
            supportsAllDrives=True
        ).execute()
        
        print(f"✅ Éxito: {nombre_original} -> PROCESADO_{nombre_csv}")

    except Exception as e:
        print(f"❌ Fallo al procesar {nombre_original}: {e}")

def main():
    service = autenticar_drive()
    if not service: return

    print("Buscando archivos en Drive...")
    
    # Buscamos archivos en la carpeta de entrada que NO tengan la palabra PROCESADO
    query = f"'{FOLDER_ID_ENTRADA}' in parents and trashed = false"
    
    results = service.files().list(
        q=query,
        fields="files(id, name, mimeType)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    
    archivos = results.get('files', [])
    print(f"Encontrados: {len(archivos)} archivos.")

    for file in archivos:
        # Evitamos carpetas y archivos ya procesados por nombre
        if file['mimeType'] == 'application/vnd.google-apps.folder' or "PROCESADO" in file['name']:
            continue
        
        print(f"Descargando para procesar: {file['name']}...")
        
        try:
            # Descarga el contenido a memoria
            request = service.files().get_media(fileId=file['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            contenido = fh.getvalue()
            nombre = file['name']

            # Lógica de procesamiento (maneja ZIPs o archivos p7z directos)
            if nombre.lower().endswith('.zip'):
                with zipfile.ZipFile(io.BytesIO(contenido), 'r') as z:
                    for p7z_name in [f for f in z.namelist() if f.endswith('.p7z')]:
                        procesar_p7z(z.read(p7z_name), p7z_name, service)
            else:
                procesar_p7z(contenido, nombre, service)

        except Exception as e:
            print(f"Error descargando {file['name']}: {e}")

if __name__ == "__main__":
    main()
