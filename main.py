import io
import csv
import zipfile
import subprocess
import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# CONFIGURACIÓN: ID de tu carpeta de destino en Drive
FOLDER_ID_SALIDA = "1WK0HaCeEtTuOOPgJbT1mtGA-uJPAJTsQ"

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
    """Autenticación para GitHub Actions usando variables de entorno."""
    try:
        # GitHub Actions inyecta el secreto en os.environ
        creds_raw = os.environ.get('GCP_SA_KEY')
        
        if not creds_raw:
            print("Error: No se encontró la variable de entorno GCP_SA_KEY.")
            return None

        creds_json = json.loads(creds_raw)
        creds = service_account.Credentials.from_service_account_info(creds_json)
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        print(f"Error de autenticación: {e}")
        return None

def subir_a_drive(service, nombre_archivo, contenido_string):
    """Sube el CSV directamente a la carpeta de Drive."""
    try:
        fh = io.BytesIO(contenido_string.encode('utf-8'))
        metadata = {'name': nombre_archivo, 'parents': [FOLDER_ID_SALIDA]}
        media = MediaIoBaseUpload(fh, mimetype='text/csv', resumable=True)
        
        file = service.files().create(
            body=metadata,
            media_body=media,
            fields='id',
            supportsAllDrives=True
        ).execute()
        print(f"Éxito: {nombre_archivo} subido (ID: {file.get('id')})")
    except Exception as e:
        print(f"Error al subir a Drive: {e}")

def procesar_p7z(contenido_p7z, nombre_p7z, service):
    """Lógica de desencriptación (OpenSSL) y transformación."""
    try:
        comando = ["openssl", "smime", "-verify", "-inform", "DER", "-noverify"]
        proceso = subprocess.run(comando, input=contenido_p7z, capture_output=True, check=True)
        
        with zipfile.ZipFile(io.BytesIO(proceso.stdout), 'r') as z:
            nombre_txt = next((n for n in z.namelist() if n.endswith('.txt')), None)
            if not nombre_txt: return
            texto = z.read(nombre_txt).decode('utf-8')

        tipo = "BREB100" if "BREB100" in nombre_txt else "BREB101"
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=_HEADERS[tipo], delimiter=';', quoting=csv.QUOTE_ALL)
        writer.writeheader()
        
        reader = csv.DictReader(io.StringIO(texto), fieldnames=_HEADERS[tipo], delimiter=';')
        for row in reader:
            writer.writerow(row)

        subir_a_drive(service, nombre_txt.replace(".txt", ".csv"), output.getvalue())
    except Exception as e:
        print(f"Error procesando {nombre_p7z}: {e}")

def ejecutar_proceso_completo():
    service = autenticar_drive()
    if not service: return

    # Escanea la carpeta raíz buscando archivos para procesar automáticamente
    archivos = [f for f in os.listdir('.') if f.lower().endswith(('.zip', '.p7z'))]
    
    if not archivos:
        print("No se encontraron archivos .zip o .p7z para procesar.")
        return

    for archivo_entrada in archivos:
        print(f"Procesando archivo: {archivo_entrada}")
        if archivo_entrada.lower().endswith('.zip'):
            with zipfile.ZipFile(archivo_entrada, 'r') as z:
                for p7z in [f for f in z.namelist() if f.endswith('.p7z')]:
                    procesar_p7z(z.read(p7z), p7z, service)
        elif archivo_entrada.lower().endswith('.p7z'):
            with open(archivo_entrada, 'rb') as f:
                procesar_p7z(f.read(), archivo_entrada, service)

if __name__ == "__main__":
    ejecutar_proceso_completo()
