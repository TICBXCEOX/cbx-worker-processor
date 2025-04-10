import csv
import json
import os
import tempfile
import uuid
import boto3
from pandas import DataFrame, ExcelWriter
from services.email_service import EmailService
from services.file_service import FileService
from configs import ACCESS_KEY, SECRET_KEY, REGION_NAME, BUCKET_NAME, ROOT_DOWNLOAD_FOLDER, WAIT_TIME_SECONDS

class AwsService:
    def __init__(self):
        self.sqs = self.sign_sqs()
    
    def sign(self):
        return boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    
    def sign_sqs(self):
        return boto3.client('sqs', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=REGION_NAME)
    
    def session(self):
        s3 = boto3.Session(aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY)
        s3_resource = s3.resource('s3')
        return s3_resource.Bucket(BUCKET_NAME)

    def upload(self, file_path, s3_path):
        try:
            s3 = self.sign()
            s3.upload_file(file_path, BUCKET_NAME, s3_path)
            return True, 'Upload s3 sucesso'
        except Exception as ex:
            msg = f'Erro ao fazer upload para o S3: {str(ex)}'
            return False, msg

    def download(self, s3_path, download_path):
        try:
            s3 = self.sign()
            s3.download_file(BUCKET_NAME, s3_path, download_path)        
            return True, 'Download S3 sucesso'
        except Exception as ex:
            # Adicione tratamento de erro adequado, como log de erro ou notificação
            msg = f'Erro ao fazer download do S3: {str(ex)}'
            return False, msg

    def get_s3_url(self, s3_path, expires=3600):        
        try:
            s3 = self.sign()
            url = s3.generate_presigned_url(ClientMethod='get_object', Params={'Bucket': BUCKET_NAME, 'Key': s3_path}, ExpiresIn=expires)
            return url, ''
        except Exception as ex:
            msg = f'Erro ao obter url do S3: {str(ex)}'
            return '', msg
                                                
    def upload_csv_by_chunks(self, data_iterator: list, folder: str = '', expires: int=3600):
        error: str = ''
        try:
            os.makedirs(ROOT_DOWNLOAD_FOLDER, exist_ok=True)

            # Criar um arquivo temporário
            with tempfile.NamedTemporaryFile(delete=True, mode='w', newline='', dir=ROOT_DOWNLOAD_FOLDER) as temp_file:
                first_chunk = True             
                
                # Escrever dados no arquivo temporário
                for chunk_df in data_iterator:
                    if first_chunk:
                        chunk_df.to_csv(temp_file.name, header=True, index=False)
                        first_chunk = False
                    else:
                        chunk_df.to_csv(temp_file.name, header=False, index=False)
                
                s3 = self.sign()
            
                # Fazer o upload do arquivo temporário para o S3
                s3_path = f'{folder}/{os.path.basename(temp_file.name)}'
                s3.upload_file(temp_file.name, BUCKET_NAME, s3_path)
                
                # Gerar a URL pré-assinada para download
                presigned_url = s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': BUCKET_NAME, 'Key': s3_path},
                    ExpiresIn=expires  # 1 hora de expiração
                )
        except Exception as ex:
            error = f'Erro de upload do arquivo {s3_path if s3_path else ""} para o S3: {str(ex)}'

        return presigned_url, error
    
    def send_message_robo(self, url, tipo, transaction_id, file_name, email, url_s3_txt_key_nf, client_id, message_group_id):
        error: str = ''
        try:
            sqs = self.sign_sqs()
            
            message_deduplication_id = str(uuid.uuid4()).replace('-', '')[:9]
            
            mensagem = {
                'transaction_id': transaction_id,
                'file_name': file_name,
                'tipo': tipo, #tipo (21=DANFE, 22=SEFAZ)
                'email': email,
                'txt_url': url_s3_txt_key_nf,
                'client_id': client_id
            }        

            response = sqs.send_message(
                QueueUrl=url,
                MessageBody=json.dumps(mensagem),
                MessageGroupId=message_group_id,
                MessageDeduplicationId=message_deduplication_id
            )
            
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:                
                return True, f"Mensagem da Fila SQS: {response['MessageId']}"
            else:
                error = f"Erro ao enviar para a Fila SQS: {response['ResponseMetadata']['HTTPStatusCode']}"                
        except Exception as ex:
            error = f"Erro ao enviar para a Fila SQS: {str(ex)}"
        return False, error    
        
    def consume_message(self, url):
        try:   
            response = self.sqs.receive_message(
                QueueUrl=url,
                MaxNumberOfMessages=1,  # Quantas mensagens recuperar
                WaitTimeSeconds=int(WAIT_TIME_SECONDS),  # Tempo máximo de espera para mensagens (long polling)
                AttributeNames=['All'],  # Se você precisar de atributos da mensagem
                MessageAttributeNames=['All']  # Atributos extras da mensagem, se houver
            )
            
            if 'Messages' not in response:
                return False, 'Nenhuma mensagem na fila'
            
            return True, response['Messages']            
        except Exception as ex:
            error = f"Erro ao consumir mensagem da fila SQS: {str(ex)}"
            return False, error
        
    def delete_message(self, url, message):
        try:
            self.sqs.delete_message(
                QueueUrl=url,
                ReceiptHandle=message['ReceiptHandle']
            )
            return True, 'Mensagem removida da Fila'
        except Exception as ex:
            msg = f'Erro ao remover mensagem da fila: {str(ex)}'
            return False, msg        