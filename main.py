def main():
    service = autenticar_drive()
    if not service: return

    query = f"'{FOLDER_ID_ENTRADA}' in parents and trashed = false"
    results = service.files().list(
        q=query, 
        fields="files(id, name, mimeType)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    
    archivos = results.get('files', [])
    print(f"Archivos encontrados: {len(archivos)}")

    for file in archivos:
        if "PROCESADO" in file['name'] or file['mimeType'] == 'application/vnd.google-apps.folder':
            continue

        print(f"Leyendo archivo de Drive: {file['name']} (Tipo: {file['mimeType']})")
        
        request = service.files().get_media(fileId=file['id'])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        contenido = fh.getvalue()
        nombre_archivo = file['name'].lower()
        mime_type = file['mimeType']

        # --- VALIDACIÓN FLEXIBLE MEJORADA ---
        # Si termina en .zip O si Google dice que es un ZIP (aunque no tenga extensión)
        if nombre_archivo.endswith('.zip') or mime_type == 'application/zip' or mime_type == 'application/x-zip-compressed':
            print(f"Procesando como ZIP: {file['name']}")
            with zipfile.ZipFile(io.BytesIO(contenido), 'r') as z_maestro:
                archivos_p7z = [f for f in z_maestro.namelist() if f.endswith('.p7z')]
                if not archivos_p7z:
                    print("No se encontraron archivos .p7z dentro del ZIP.")
                for p7z_interno in archivos_p7z:
                    procesar_p7z_nuclear(z_maestro.read(p7z_interno), p7z_interno, service)
        
        # Si es un .p7z directo
        elif nombre_archivo.endswith('.p7z') or 'pkcs7' in mime_type:
            procesar_p7z_nuclear(contenido, file['name'], service)
        
        # CASO ESPECIAL: Si no tiene extensión pero pesa y es binario, intentamos tratarlo como P7Z directo
        else:
            print(f"Intentando procesar {file['name']} como P7Z por descarte...")
            procesar_p7z_nuclear(contenido, file['name'], service)
