import io
import csv
import zipfile
import subprocess
import os

# Encabezados oficiales segun el tipo de archivo
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

def procesar_archivo_p7z(contenido_p7z, nombre_p7z):
    """
    Realiza el proceso nuclear: remocion de firma P7Z, descompresion de ZIP interno
    y conversion del TXT resultante a formato CSV.
    """
    try:
        # 1. Remocion de firma PKCS7 usando OpenSSL
        comando = ["openssl", "smime", "-verify", "-inform", "DER", "-noverify"]
        proceso = subprocess.run(comando, input=contenido_p7z, capture_output=True, check=True)
        datos_zip_interno = proceso.stdout
        
        # 2. Apertura de ZIP interno y extraccion de TXT
        with zipfile.ZipFile(io.BytesIO(datos_zip_interno), 'r') as z_interno:
            nombre_txt = next((n for n in z_interno.namelist() if n.endswith('.txt')), None)
            if not nombre_txt:
                print(f"Error: No se encontro TXT dentro de {nombre_p7z}")
                return
            
            with z_interno.open(nombre_txt) as f_txt:
                texto_plano = f_txt.read().decode('utf-8')

        # 3. Mapeo de encabezados y transformacion a CSV
        tipo = "BREB100" if "BREB100" in nombre_txt else "BREB101"
        headers_actuales = _HEADERS[tipo]
        
        output_csv = io.StringIO()
        writer = csv.DictWriter(output_csv, fieldnames=headers_actuales, delimiter=';', quoting=csv.QUOTE_ALL)
        writer.writeheader()
        
        reader = csv.DictReader(io.StringIO(texto_plano), fieldnames=headers_actuales, delimiter=';')
        filas = 0
        for row in reader:
            writer.writerow(row)
            filas += 1

        # 4. Escritura de archivo CSV final
        nombre_final = nombre_txt.replace(".txt", ".csv")
        with open(nombre_final, "w", encoding="utf-8") as f_out:
            f_out.write(output_csv.getvalue())
        
        print(f"Procesado: {nombre_p7z} -> Generado: {nombre_final} | Filas: {filas}")

    except Exception as e:
        print(f"Error procesando {nombre_p7z}: {str(e)}")

def validador_entrada_flexible(ruta_archivo):
    """
    Valida la extension de entrada. Si es ZIP, itera sus archivos P7Z.
    Si es P7Z, lo procesa directamente.
    """
    if not os.path.exists(ruta_archivo):
        print(f"Error: El archivo {ruta_archivo} no existe.")
        return

    extension = ruta_archivo.lower()

    # Caso 1: Archivo ZIP maestro
    if extension.endswith('.zip'):
        print(f"Detectado archivo ZIP maestro: {ruta_archivo}")
        with zipfile.ZipFile(ruta_archivo, 'r') as z_maestro:
            archivos_p7z = [f for f in z_maestro.namelist() if f.endswith('.p7z')]
            for nombre_p7z in archivos_p7z:
                with z_maestro.open(nombre_p7z) as f_p7z:
                    procesar_archivo_p7z(f_p7z.read(), nombre_p7z)

    # Caso 2: Archivo P7Z individual
    elif extension.endswith('.p7z'):
        print(f"Detectado archivo P7Z individual: {ruta_archivo}")
        with open(ruta_archivo, 'rb') as f_p7z:
            procesar_archivo_p7z(f_p7z.read(), ruta_archivo)

    else:
        print("Error: Extension no soportada (use .zip o .p7z).")
