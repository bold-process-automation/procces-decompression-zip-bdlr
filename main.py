import io
import csv
import zipfile
import subprocess
import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# ==========================================
# CONFIGURACIÓN
# ==========================================
FOLDER_ID_ENTRADA = "1TB8fPJkli8-Qzke28VAugn2EyqQFGxmE"
FOLDER_ID_SALIDA = "1WK0HaCeEtTuOOPgJbT1mtGA-uJPAJTsQ"

_HEADERS = {
    "BREB100": ["MOL_ID", "TRANSACTION_ID", "TRANSACTION_DATE", "TRANSACTION_MOL_DATE", "TRANSACTION_CREATION_DATE", "TRANSACTION_AMOUNT", "AUTHORIZER_PROVIDER", "NIT_AUTHORIZER_PROVIDER", "RECEIVER_PROVIDER", "NIT_RECEIVER_PROVIDER", "TRANSACTION_STATUS", "TRANSACTION_STATUS_CODE", "TRANSACTION_ERROR_CODE"],
    "BREB101": ["TRANSACTION_DATE", "TRANSACTION_ID", "TRANSACTION_STATUS", "TRANSACTION_STATUS_CODE", "TRANSACTION_DETAIL", "TRANSACTION_TYPE", "TRANSACTION_AMOUNT"]
}

def autenticar_drive():
    try:
        # Prioridad 1: GitHub Secrets / Prioridad 2: Colab Userdata
        creds_raw = os.environ.get('GCP_SA_KEY')
        if not creds_raw:
            try:
                from google.colab import userdata
                creds_raw = userdata.get('GCP_SA_KEY')
            except:
                pass

        if not creds_raw:
            print("❌ Error: No se encontró la credencial GCP_SA_KEY.")
            return None

        creds_json = json.loads(creds_raw)
        creds = service_account.Credentials.from_service_account_info(creds_json)
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        print(f"❌ Error de autenticación: {e}")
        return None

def descargar_archivo(service, file_id):
    """Descarga los bytes del archivo desde Drive."""
    try:
        file_metadata = service.files().get(fileId=file_id, supportsAllDrives=True).execute()
        nombre = file_metadata['name']
        
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        return fh.getvalue(), nombre
    except Exception as e:
        print(f"❌ Error descargando {file_id}: {e}")
        return None, None

def procesar_y_subir(contenido_binario, nombre_archivo, service):
    """Lógica original: OpenSSL -> ZIP -> CSV -> Drive."""
    try:
        print(f"🔓 Desencriptando {nombre_archivo}...")
        proceso = subprocess.run(
            ["openssl", "smime", "-verify", "-inform", "DER", "-noverify"],
            input=contenido_binario, capture_output=True, check=True
        )

        with zipfile.ZipFile(io.BytesIO(proceso.stdout), 'r') as z:
            nombre_txt = next((n for n in z.namelist() if n.endswith('.txt')), None)
            if not nombre_txt:
                print(f"⚠️ No se encontró archivo .txt dentro de {nombre_archivo}")
                return
            texto = z.read(nombre_txt).decode('utf-8')

        print(f"📊 Transformando {nombre_txt}...")
        tipo = "BREB100" if "BREB100" in nombre_txt else "BREB101"
        output_csv = io.StringIO()
        writer = csv.DictWriter(output_csv, fieldnames=_HEADERS[tipo], delimiter=';', quoting=csv.QUOTE_ALL)
        writer.writeheader()
        reader = csv.DictReader(io.StringIO(texto), fieldnames=_HEADERS[tipo], delimiter=';')
        for row in reader:
            writer.writerow(row)

        nombre_final = nombre_txt.replace(".txt", ".csv")
        fh_upload = io.BytesIO(output_csv.getvalue().encode('utf-8'))
        metadata = {'name': f"PROCESADO_{nombre_final}", 'parents': [FOLDER_ID_SALIDA]}
        media = MediaIoBaseUpload(fh_upload, mimetype='text/csv')
        
        service.files().create(body=metadata, media_body=media, supportsAllDrives=True).execute()
        print(f"✅ Éxito: {nombre_final} subido a Drive.")

    except Exception as e:
        print(f"❌ Error procesando {nombre_archivo}: {e}")

if __name__ == "__main__":
    drive_service = autenticar_drive()
    
    if drive_service:
        # LISTAR ARCHIVOS DE LA CARPETA DE ENTRADA
        query = f"'{FOLDER_ID_ENTRADA}' in parents and trashed = false"
        results = drive_service.files().list(
            q=query, 
            fields="files(id, name, mimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        archivos = results.get('files', [])
        print(f"📂 Archivos encontrados: {len(archivos)}")

        for file in archivos:
            # Filtro: No procesar carpetas ni archivos ya marcados como PROCESADO
            if file['mimeType'] == 'application/vnd.google-apps.folder' or "PROCESADO" in file['name']:
                continue
            
            print(f"--- Iniciando con: {file['name']} ---")
            binario, nombre = descargar_archivo(drive_service, file['id'])
            
            if binario:
                # Caso ZIP: Entrar y buscar P7Z (como estaba en tu código)
                # También incluimos la lógica de MimeType por si el archivo no tiene extensión .zip
                is_zip = nombre.lower().endswith('.zip') or file['mimeType'] == 'application/zip'
                
                if is_zip:
                    try:
                        with zipfile.ZipFile(io.BytesIO(binario), 'r') as z_master:
                            archivos_p7z = [f for f in z_master.namelist() if f.endswith('.p7z')]
                            for p7z in archivos_p7z:
                                procesar_y_subir(z_master.read(p7z), p7z, drive_service)
                    except Exception as e:
                        # Si falla como ZIP, intentamos procesarlo como P7Z directo por si acaso
                        procesar_y_subir(binario, nombre, drive_service)
                else:
                    # Caso P7Z directo
                    procesar_y_subir(binario, nombre, drive_service)

        print("🏁 Proceso finalizado.")
