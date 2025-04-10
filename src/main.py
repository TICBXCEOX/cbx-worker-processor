import shutil
import uuid

from pathlib import Path
from configs import *
from services.aws_service import AwsService
from services.email_service import EmailService
from services.file_service import FileService
from services.logger_service import LoggerService
from services.nf_service import NotaFiscalService
from os.path import join

class WorkerProcessor:
    def __init__(self):
        self.aws_service = AwsService()
        self.file_service = FileService()
        self.logger_service = LoggerService()
                    
    def run(self):
        try:
            # baixa arquivo zip do S3
            # descompacta o arquivo zip
            # processa os arquivos do zip
            # envia os arquivos processados para o S3
            # envia email com os links dos arquivos processados
            
            if DEBUG:               
                transaction_id = '4f3f8a5b-a8ec-493f-87ff-144205ee115d'
                zip_name = 'Entrada-150 registros'
                tipo = 22
                email = 'edzatarin@gmail.com'
                s3_path = 'input/Entrada-150 registros-7aee8326f.zip'
                client_id = 1
                message_group = 'CBX'
                user_id = 139
                send_queue = True
                request_origin = 'WEB'
            else:
                # container docker - prod
                s3_path = os.getenv("S3_PATH")        
                transaction_id = os.getenv("TRANSACTION_ID")
                zip_name = os.getenv("FILE_NAME")
                
                tipo = os.getenv("TIPO")
                if tipo:
                    tipo = int(tipo)
                else:
                    return False, "Tipo não informado"
                    
                client_id = os.getenv("CLIENT_ID")
                if client_id:
                    client_id = int(client_id)
                else:
                    return False, "Client ID não informado"
                    
                request_origin = os.getenv("REQUEST_ORIGIN")
                
                # dados do usuário
                message_group = os.getenv("MESSAGE_GROUP")
                email = os.getenv("EMAIL")
                user_id = os.getenv("USER_ID")
                if user_id:
                    user_id = int(user_id)
                else:
                    return False, "User ID não informado"
                    
                send_queue = os.getenv("SEND_QUEUE")
                
            userdata = self.get_userdata(user_id, email, send_queue, message_group)
                
            # cria pasta para o arquivo zip        
            hash_id = str(uuid.uuid4()).replace('-', '')[:9]
            folder = join(ROOT_DOWNLOAD_FOLDER, zip_name, hash_id)
            sucesso, msg = self.file_service.create_folder(folder)
            self.logger_service.info(msg)
            if not sucesso:
                self.logger_service.error(msg)
                return False, msg
                
            # baixa arquivo zip do S3
            only_file_name = Path(s3_path).name
            download_path = join(folder, only_file_name)
            sucesso, msg =self.aws_service.download(s3_path, download_path)
            self.logger_service.info(msg)
            if not sucesso:
                self.logger_service.error(msg)
                return False, msg
            
            nf_service = NotaFiscalService()
                
            result = nf_service.unzip_file_and_process(userdata, s3_path, zip_name, download_path, tipo,
                                                        transaction_id, request_origin, client_id)
            status = result["status"]
            erros = result["erros"]        
            if not status and erros:
                email_service = EmailService()
                error_str = email_service.get_flat_html_from_list(erros)
                if not DEBUG:
                    sucesso, code, msg = email_service.send_error(email, error_str, zip_name, transaction_id)
                    if not sucesso:
                        error_str = f"{msg}\n{code}"
                        return False, error_str                        
                error_str = f'{email_service.get_flat_str_from_list(erros)}'
                return False, error_str
            else:            
                return True, 'Sucesso! Não há erros'
        except Exception as ex:
            return False, str(ex)
        finally:            
            if folder and os.path.exists(folder):
                shutil.rmtree(folder) 

            if download_path and os.path.exists(download_path):
                os.remove(download_path) 
    
    def iniciar_worker(self):
        try:
            self.logger_service.info("<<<--- INÍCIO PROCESSOR --->>>")
            self.logger_service.info("Modo DEBUG: " + str(DEBUG))
            sucesso, msg = self.run()
            self.logger_service.info(msg)
            return sucesso, msg        
        except Exception as ex:
            msg = f"ERROR: {str(ex)}"            
            self.logger_service.error(msg)
            return False, msg
        finally:
            self.logger_service.info("<<<--- PROCESSOR FINALIZADO --->>>")

    def get_userdata(self, user_id: int, email: str, send_queue: bool, message_group: str):
        # mesmo padrao de estrutura da api
        userdata = {
            "email": email,
            "user": [
                user_id,
                email,
                "password",
                {
                    "name": "User name",
                    "clients": [],
                    "send_queue": send_queue,
                    "message_group": message_group
                },
                True,
                "admin"
            ]
        }
        return userdata            

if __name__ == "__main__":
    worker = WorkerProcessor()
    worker.iniciar_worker()