import os
import psycopg2
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()
dbname = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
azure_storage_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# URL da conta do Blob Storage com o SAS token
account_url_with_sas = f"https://atender.blob.core.windows.net?sp={azure_storage_connection_string}"

# Nome do container
container_name = "atender"

# Data de criação (timestamp do dia de hoje)
create_date = datetime.now()

# Conexão com o banco de dados PostgreSQL
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)

# Cria um cursor
cur = conn.cursor()

# Cria um BlobServiceClient usando a URL da conta com o SAS token
blob_service_client = BlobServiceClient(account_url=account_url_with_sas)

# Cria um ContainerClient
container_client = blob_service_client.get_container_client(container_name)

# Lista todos os blobs no container
blob_list = container_client.list_blobs()

# Itera sobre os blobs e extrai as informações
for blob in blob_list:
    blob_path_parts = blob.name.split("/")

    if len(blob_path_parts) >= 2:
        doc_type = blob_path_parts[1]
        doc_name = blob_path_parts[-1]
        blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob.name}"
        id_team = blob_path_parts[0]

        cur.execute("SELECT id_type FROM documents_type WHERE name = %s", (doc_type,))
        id_type_result = cur.fetchone()

        if id_type_result:
            id_type = id_type_result[0]
        else:
            print(f"Tipo de documento '{doc_type}' não encontrado no banco de dados.")
            continue

        cur.execute("SELECT company_id FROM company WHERE name_blob = %s", (id_team,))
        id_team_result = cur.fetchone()

        if id_team_result:
            id_team = id_team_result[0]
        else:
            print(f"Equipe '{id_team}' não encontrada no banco de dados.")
            continue

        # Verifica se a URL do documento já existe no banco de dados
        cur.execute("SELECT COUNT(*) FROM documents WHERE url_doc = %s", (blob_url,))
        url_exists = cur.fetchone()[0]

        if url_exists > 0:
            print(f"A URL do documento '{blob_url}' já existe na tabela 'documents'.")
            continue

        # Insere os dados na tabela documents
        cur.execute(
            "INSERT INTO documents (id_type, doc_name, url_doc, id_team, create_date) VALUES (%s, %s, %s, %s, %s)",
            (int(id_type), doc_name, blob_url, int(id_team), create_date),
        )
    else:
        print(
            "Caminho do blob não contém informações suficientes para determinar o tipo e o ID da equipe."
        )

# Commit da transação
conn.commit()

# Fecha o cursor e a conexão
cur.close()
conn.close()
