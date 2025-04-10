from datetime import datetime
import json
from psycopg2.extras import execute_batch
from services.utils import *

class SefazService:
    def __init__(self):
        pass

    def insert_sefaz(self, rows):        
        with get_db_connection() as connection, connection.cursor() as cursor:
            try:
                query = """
                    INSERT INTO cbx.sefaz (
                        properties, data_emissao, ie_emissor,
                        ie_destinatario, cnpj_cpf_emissor, cnpj_cpf_destinatario,
                        razao_social_emissor, chave,
                        situacao, fonte, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """

                data = [
                    (
                        json.dumps(row.get("properties", {})),  
                        row.get("data_emissao", None),
                        row.get("ie_emissor", None),
                        row.get("ie_destinatario", None),
                        row.get("cnpj_cpf_emissor", None),
                        row.get("cnpj_cpf_destinatario", None),
                        row.get("razao_social_emissor", None),
                        row.get("chave", None),
                        row.get("situacao", None),
                        "SEFAZ MT",
                        datetime.now().isoformat(),
                    )
                    for row in rows
                ]

                execute_batch(cursor, query, data)  # Optimized batch insert
                connection.commit()

                return True, "Registros atualizados na base de dados do Sefaz"
            except Exception as e:
                status, msg = False, f'Erro ao inserir na base de dados do Sefaz: {str(e)}'
                connection.rollback()            
                connection = None
            return status, msg
