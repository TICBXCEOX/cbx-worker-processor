from pathlib import Path
import zipfile
import pandas as pd
import tempfile

from configs import DEBUG, SQS_PROCESSAMENTO_RENOVABIO, SQS_PROCESSAMENTO_RENOVABIO_DLQ
from services.danfe_service import DanfeService
from services.aws_service import AwsService
from sqlalchemy import text

from services.file_process_log_service import FileProcessLogService
from services.nf_chave_service import NotaFiscalChaveService
from services.nf_email_service import NotaFiscalEmailService
from services.nf_excel_service import NotaFiscalExcelService
from services.nf_logger_service import NotaFiscalLoggerService
from services.nf_xml_service import NotaFiscalXmlService
from services.robo_chaves_service import RoboChavesService
        
class NotaFiscalProcessorService:
    def __init__(self):
        self.FOLDER_TEMP = "temp"
        self.FOLDER_INPUT = "input"
        self.FOLDER_PROCESS = "process"
        self.FOLDER_OUTPUT = "output"
                
        self.ok = True
        self.total_files = 0
        self.total_erros = 0
        
        self.aws_service = AwsService()
        self.nf_email_service = NotaFiscalEmailService()
        self.nf_logger_service = NotaFiscalLoggerService()
        self.nf_xml_service = NotaFiscalXmlService()
        self.nf_excel_service = NotaFiscalExcelService()
        self.nf_chave_service = NotaFiscalChaveService()
        self.robo_chaves_service = RoboChavesService()
        self.danfe_service = DanfeService()
        
        self.SQS_PROCESSAMENTO_RENOVABIO = SQS_PROCESSAMENTO_RENOVABIO
        self.SQS_PROCESSAMENTO_RENOVABIO_DLQ = SQS_PROCESSAMENTO_RENOVABIO_DLQ
        
    # define variaveis para processamento
    def setup(self,
              request_origin: str,
              transaction_id: str, 
              tipo_processo: int, 
              zip_name: str,
              full_path_zip_filename: str, 
              folder_extract_zip: str,              
              selected_client: str,
              user_email: str,
              message_group_id: str,
              user_id: int):
        self.request_origin = request_origin
        self.transaction_id = transaction_id
        self.tipo = tipo_processo
        self.zip_name = zip_name
        self.full_path_zip_filename = full_path_zip_filename
        self.folder_extract_zip = folder_extract_zip
        self.user_email = user_email
        self.selected_client = selected_client
        self.message_group_id = message_group_id
        self.user_id = user_id        
        self.nf_logger_service.clear_monitoring()
        
    def track_monitoring(self, msg: str):
        self.nf_logger_service.track_monitoring(msg)
        
    def track_error(self, msg_or_msgs):
        self.nf_logger_service.track_error(msg_or_msgs)
    
    def track_log(self, msg_or_msgs):
        self.nf_logger_service.track_log(msg_or_msgs)
        
    def separator(self):
        self.nf_logger_service.separator()        
        
    def get_logs(self):
        return self.nf_logger_service.logs
    
    def get_errors(self):
        return self.nf_logger_service.errors    
    
    def start(self):
        tipo_str = self.get_tipo_str(self.tipo)
        self.track_log(f'Início Processamento do Arquivo {tipo_str}')
        self.track_log(f'Arquivo: {self.zip_name}')
        #self.separator()
    
    def end(self):
        self.track_log(f'Fim do Processamento do Arquivo')
    
    # 1. extrai o arquivo zip
    def unzip(self):
        if not self.full_path_zip_filename:
            self.ok = False
            raise Exception('Necessário iniciar ambiente do processamento')
        
        try:
            self.track_log(f'Extraindo arquivo ZIP')
            with zipfile.ZipFile(self.full_path_zip_filename, 'r') as zip_ref:
                zip_ref.extractall(self.folder_extract_zip)
                self.track_log(f'ZIP extraído')
        except Exception as ex:
            self.track_monitoring(f"Erro ao extrair arquivo - Erro: {str(ex)}")
            self.ok = False

    # 2. processa os pdfs do zip
    def process_danfes(self):
        if not self.ok:
            return

        self.track_log("Processando DANFES")
        result = self.danfe_service.processar_danfes(self.folder_extract_zip, self.full_path_zip_filename)

        status = result.get("status", False)
        erros = result.get("erros", [])
        self.total_files = result.get("total_files", 0)        
        df = result['df'] if 'df' in result and result['df'] is not None and not result['df'].empty else None
        if not status:
            self.track_monitoring(f"Erro ao processar DANFES. Total de Arquivos: {self.total_files} | Total de Erros: {len(erros)}")
            self.ok = False
        else:
            self.track_log(f'DANFES processadas. Total Arquivos: {self.total_files} | Total de Erros: {len(erros)}')
        if erros:
            self.track_log(erros)
            self.track_error(erros)            
        return df
    
    # 2. processa xmls do zip (insumos)
    def processar_nfs_insumos(self):
        if not self.ok:
            return
        
        self.track_log("Processando NFs INSUMO")
        result = self.nf_xml_service.processar_nfs_insumos(self.folder_extract_zip, self.full_path_zip_filename)
        
        status = result.get("status", False)
        erros = result.get("erros", [])
        self.total_files = result.get("total_files", 0)        
        df = result['df'] if 'df' in result and not result['df'].empty else None
        if not status:
            self.track_monitoring(f"Erro ao processar INSUMOS. Total de Arquivos: {self.total_files} | Total de Erros: {len(erros)}")
            self.ok = False
        else:
            self.track_log(f'NFs INSUMO processadas. Total Arquivos: {self.total_files} | Total de Erros: {len(erros)}')
        if erros:
            self.track_log(erros)
            self.track_error(erros)            
        return df        
    
    # 2. processa xmls do zip (milho)
    def processar_nfs_milho(self):
        if not self.ok:
            return
        
        self.track_log("Processando NFs MILHO")
        result = self.nf_xml_service.processar_nfs_milho(self.folder_extract_zip, self.full_path_zip_filename)
        
        status = result.get("status", False)
        erros = result.get("erros", [])
        self.total_files = result.get("total_files", 0)        
        df = result['df'] if 'df' in result and not result['df'].empty else None
        if not status:
            self.track_monitoring(f"Erro ao processar NFs MILHO. Total de Arquivos: {self.total_files} | Total de Erros: {len(erros)}")
            self.ok = False
        else:
            self.track_log(f'NFs MILHO processadas. Total Arquivos: {self.total_files} | Total de Erros: {len(erros)}')
        if erros:
            self.track_log(erros)
            self.track_error(erros)            
        return df
    
    # 2. processa xmls do zip (cbios)
    def processar_nfs_cbios(self):
        if not self.ok:
            return
        
        self.track_log("Processando NFs CBIOS")
        result = self.nf_xml_service.processar_nfs_cbios(self.folder_extract_zip, self.full_path_zip_filename)
        
        status = result.get("status", False)
        erros = result.get("erros", [])
        self.total_files = result.get("total_files", 0)        
        df = result['df'] if 'df' in result and not result['df'].empty else None
        if not status:
            self.track_monitoring(f"Erro ao processar NFs CBIOS. Total de Arquivos: {self.total_files} | Total de Erros: {len(erros)}")
            self.ok = False
        else:
            self.track_log(f'NFs CBIOS processadas. Total Arquivos: {self.total_files} | Total de Erros: {len(erros)}')
        if erros:
            self.track_log(erros)
            self.track_error(erros)
        return df

    # 2. processa excel (sefaz)
    def processar_sefaz(self):
        if not self.ok:
            return

        self.track_log("Processando SEFAZ")
        result = self.nf_excel_service.processar_sefaz(self.folder_extract_zip, self.full_path_zip_filename)

        status = result.get("status", False)
        erros = result.get("erros", [])
        self.total_files = result.get("total_files", 0)        
        df = result['df'] if 'df' in result and result['df'] is not None and not result['df'].empty else None
        if not status:
            self.track_monitoring(f"Erro ao processar SEFAZ. Total de Arquivos: {self.total_files} | Total de Erros: {len(erros)}")
            self.ok = False
        else:
            self.track_log(f'SEFAZ processadas. Total Arquivos: {self.total_files} | Total de Erros: {len(erros)}')
        if erros:
            self.track_log(erros)
            self.track_error(erros)            
        return df
    
    # 2. processa chaves (txt, csv)
    def processar_chaves(self):
        if not self.ok:
            return

        self.track_log("Processando CHAVES")
        result = self.nf_chave_service.processar_chaves(self.folder_extract_zip, self.full_path_zip_filename)

        status = result.get("status", False)
        erros = result.get("erros", [])
        self.total_files = result.get("total_files", 0)        
        df = result['df'] if 'df' in result and result['df'] is not None and not result['df'].empty else None
        if not status:
            self.track_monitoring(f"Erro ao processar CHAVES. Total de Arquivos: {self.total_files} | Total de Erros: {len(erros)}")
            self.ok = False
        else:
            self.track_log(f'CHAVES processadas. Total Arquivos: {self.total_files} | Total de Erros: {len(erros)}')
        if erros:
            self.track_log(erros)
            self.track_error(erros)            
        return df
      
    # 3. sincroniza chaves no banco de dados para controle
    def sync_key_nf(self, df, column_key_nf: str = 'key_nf'):
        if DEBUG:
            return df
        if not self.ok:
            return df
             
        # validar chaves banco de dados (somente chaves não processadas)
        self.track_log(f'Sincronizando Chaves no banco de dados')
        total_antes = len(df)        
        df, error = self.robo_chaves_service.sync_key_nf(self.transaction_id, df, column_key_nf)
        if error:
            self.track_monitoring(error)
            self.ok = False
        else:
            if df.empty:
                self.track_log("Sem Chaves para processar.")
                self.ok = False
            else:                
                self.track_log(f'Chaves Sincronizadas. Total Chaves: {str(total_antes)} - Sincronizadas: {len(df)}')
        return df

    # 4. filtrar somente nfs sincronizadas
    def filter_by_df_sync(self, df, df_sync, column_key_nf: str = 'key_nf'):
        if DEBUG:
            return df     
        if not self.ok:
            return df
        df = df[df[column_key_nf].isin(df_sync['key_nf'])]
        if df.empty:
            self.track_log("Sem Chaves para processar.")
            self.ok = False        
        return df
    
    # 4. obtem a coluna chave no df
    def get_key_col(self, tipo):
        key_nf_column = 'key_nf'
        if tipo == 21:
            key_nf_column = 'CHAVE'
        if tipo == 22:
            key_nf_column = 'CHAVE_DE_ACESSO'
        if tipo == 23:
            key_nf_column = 'CHAVE'            
        return key_nf_column

    # 4. upload zip to s3
    def upload_zip_s3(self):
        if not self.ok:
            return
                       
        self.track_log(f'Uploading ZIP para o S3')
        # upload zip to s3
        s3_path = self.get_s3_path(self.full_path_zip_filename, self.FOLDER_INPUT)
        
        awsOk, awsmsg = self.aws_service.upload(self.full_path_zip_filename, s3_path)
        if not awsOk:
            self.track_monitoring(awsmsg)
        else:
            self.track_log(f'ZIP upado S3')
        return s3_path
    
    # 5. upload excel to s3
    def upload_excel_nf_s3(self, df):
        if not self.ok:
            return
                        
        self.track_log(f'Gerando e Uploading Excel para o S3')
        s3_path = ''
                
        try:
            # nome do arquivo sem extensão
            prefix = f"{Path(self.full_path_zip_filename).stem}" # facilitar o entendimento, nome do excel vai ser o mesmo nome do zip
                       
            # nome para o arquivo no S3
            s3_path = self.get_s3_path(prefix, self.FOLDER_OUTPUT, 'xlsx')
                                                    
            # Create a temporary file for intermediate storage
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=True) as temp_file:
                # danfes
                if self.tipo == 21:
                    dfx: pd.DataFrame = df.copy()
                    dfx.columns = ['CPF/CNPJ', 'CHAVE', "IE_DESTINATARIO", "IE_EMISSOR", 
                                   "CPF/CNPJ EMISSOR", "ANO/MES EMISSAO", "TIPO PROCESSAMENTO", 
                                   "NR. NOTA FISCAL", "DATA EMISSÃO", 'ARQUIVO', 'STATUS', 'Nr de Páginas']
                    with pd.ExcelWriter(temp_file.name) as writer:
                        dfx.to_excel(writer, sheet_name='DANFEs', index=False, header=True)
                # sefaz
                elif self.tipo == 22:
                    #DATA EMISSÃO	SÉRIE	NUMERO NOTA FISCAL	CHAVE DE ACESSO	NATUREZA DE OPERAÇÃO	TIPO DE EMISSÃO	NUMR DE PROTOCOLO	DATA AUTORIZAÇÃO	SITUAÇÃO	CNPJ/CPF	NOME/RAZÃO SOCIAL	INSCRIÇÃO ESTADUAL	NOME FANTASIA	UF	CNPJ/CPF	INSCRIÇÃO ESTADUAL	NOME/RAZÃO SOCIAL	UF	VALR TOTAL BASE DE CÁLCULO	VALR TOTAL ICMS	VALR TOTAL BC  S. T.	VALR TOTAL ICMS ST	VALR TOTAL PRODUTO	VALR TOTAL FRETE	VALR TOTAL NOTA FISCAL	VALR TOTAL SERVIÇO
                    dfx: pd.DataFrame = df.copy()
                    dfx.columns = [
                        "DATA EMISSÃO", "SÉRIE", "NUMERO NOTA FISCAL", "CHAVE DE ACESSO", 
                        "NATUREZA DE OPERAÇÃO", "TIPO DE EMISSÃO", "NUMR DE PROTOCOLO", 
                        "DATA AUTORIZAÇÃO", "SITUAÇÃO", "CNPJ/CPF", "NOME/RAZÃO SOCIAL", 
                        "INSCRIÇÃO ESTADUAL", "NOME FANTASIA", "UF", "CNPJ/CPF", 
                        "INSCRIÇÃO ESTADUAL", "NOME/RAZÃO SOCIAL", "UF", 
                        "VALR TOTAL BASE DE CÁLCULO", "VALR TOTAL ICMS", 
                        "VALR TOTAL BC S. T.", "VALR TOTAL ICMS ST", 
                        "VALR TOTAL PRODUTO", "VALR TOTAL FRETE", 
                        "VALR TOTAL NOTA FISCAL", "VALR TOTAL SERVIÇO"
                    ]
                    with pd.ExcelWriter(temp_file.name) as writer:
                        dfx.to_excel(writer, sheet_name='SEFAZ', index=False, header=True)
                # chaves
                elif self.tipo == 23:
                    dfx: pd.DataFrame = df.copy()
                    dfx.columns = ["CHAVE"]
                    with pd.ExcelWriter(temp_file.name) as writer:
                        dfx.to_excel(writer, sheet_name='CHAVE', index=False, header=True)
                # xmls
                else:                    
                    df_com_grupo: pd.DataFrame = df[df['grupo'].notna()]
                    df_sem_grupo: pd.DataFrame = df[df['grupo'].isna()]
                
                    # SEPARA EM DUAS ABAS, COM BASE NO CAMPO DO GRUPO DO NCM
                    with pd.ExcelWriter(temp_file.name) as writer:
                        df_com_grupo.to_excel(writer, sheet_name='com_grupo_classificado', index=False, header=True)
                        df_sem_grupo.to_excel(writer, sheet_name='sem_grupo', index=False, header=True)

                temp_file.seek(0)
                awsOk, awsmsg = self.aws_service.upload(temp_file.name, s3_path)
                if not awsOk:
                    self.track_monitoring(awsmsg)
                else:
                    self.track_log(f'Excel gerado e upado para o S3. Nome: {temp_file.name}')
            return s3_path
        except Exception as ex:
            self.track_monitoring(f"Erro ao gerar e upar Excel para o S3. Erro: {str(ex)}")
            self.ok = False
        return s3_path    
    
    # 6. get input url
    def get_input_url(self, s3_path_zip: str):
        if not self.ok:
            return
        
        # obtem url zip S3 (input)
        self.track_log(f'Obtendo URL S3 Zip')
        input_url, error = self.aws_service.get_s3_url(s3_path_zip)
        if error:
            self.track_monitoring(error)
        else:
            self.track_log(f'URL S3 Zip: {input_url}')
        return input_url
    
    # 7. get output url
    def get_output_url(self, s3_path_excel):
        if not self.ok:
            return
        
        # obtem url excel S3 (output)
        self.track_log(f'Obtendo URL S3 Excel')
        output_url, error = self.aws_service.get_s3_url(s3_path_excel)
        if error:
            self.track_monitoring(error)
        else:
            self.track_log(f'URL S3 Excel: {output_url}')
        return output_url
    
    # 8.generate and uploading txt file with keys nf
    def generate_txt_chaves_s3(self, df, column_key_nf: str = 'key_nf'):
        if not self.ok:
            return
        
        self.track_log(f'Gerando e Uploading TXT Chaves no S3')
        df = df[[column_key_nf]].reset_index(drop=True)
        txt_url, error = self.aws_service.upload_csv_by_chunks([df], folder=self.FOLDER_PROCESS, expires=3600)
        if error:
            self.track_monitoring(error)
            self.track_monitoring('Necessário processar o arquivo zip novamente')
            self.ok = False
        else:
            self.track_log(f'Url S3 TXT: {txt_url}')
        return txt_url
    
    # 9. send to SQS
    def send_to_queue_robo(self, txt_url):
        if not self.ok:
            return
        if not txt_url:
            self.track_log('Txt das chaves não gerado')
            self.ok = False
            return
        
        # Envia para fila SQS (transaction_id, email, url_s3_txt_key_nf, client_id, group id)
        self.track_log(
            f"Enviando dados para a fila - Transaction Id: {self.transaction_id} "
            f"Tipo: {self.get_tipo_str(self.tipo)} "
            f"Email: {self.user_email} "
            f"Url Chaves: {txt_url if txt_url else 'no url'} "
            f"Cliente Id: {self.selected_client} "
            f"Message Group Id: {self.message_group_id}")                    
        
        file_name = Path(self.zip_name).stem
        status, msg = self.aws_service.send_message_robo(self.SQS_PROCESSAMENTO_RENOVABIO,
                                                    self.tipo,
                                                    self.transaction_id,
                                                    file_name,
                                                    self.user_email, 
                                                    txt_url, 
                                                    self.selected_client, 
                                                    self.message_group_id)
        if status:
            self.track_log(msg)
            self.track_log('Dados enviados para a Fila')
        else:
            self.track_monitoring(msg)
    
    # 10. envia e-mail do processamento
    def send_email_process(self, input_url, output_url):
        if DEBUG:
            return
        
        self.track_log(f'Enviando e-mail do processamento do arquivo ZIP {self.zip_name} para {self.user_email}')
        # pega o tipo
        tipo_str = self.get_tipo_str(self.tipo)
        if not self.ok:
            # envia e-mail do input (zip) e ouptut (excel) para o usuário
            body_html, body_text = self.nf_email_service.get_body_no_processing(input_url, self.transaction_id, self.get_errors())
            subject = self.nf_email_service.get_subject_no_processing(tipo_str, self.zip_name, self.request_origin)
        else:
            # envia e-mail do input (zip) e ouptut (excel) para o usuário
            body_html, body_text = self.nf_email_service.get_body_processing(input_url, 
                output_url,
                self.total_files,
                self.total_files - len(self.get_errors()),
                len(self.get_errors()),
                self.transaction_id,
                '')
            subject = self.nf_email_service.get_subject_processing(tipo_str, self.zip_name, self.request_origin)
            
        sucesso, code, msg = self.nf_email_service.send_email(self.user_email, self.zip_name, body_html, body_text, subject)
        if sucesso:
            self.track_log(f'Email enviado para {self.user_email}')
            return True
        else:
            self.track_monitoring(msg)
            return False            
        # error, res = self.nf_email_service.send_email(self.user_email, self.zip_name, body_html, body_text, subject)    
        # if error:
        #     self.track_monitoring(error)
        #     return False
        # else:
        #     self.track_log(f'Email enviado para {self.user_email}')
        #     return True
    
    # 11. registra log
    def log_process(self, input_url, output_url):
        # registra processo no banco de dados
        self.track_log(f'Registrando LOG no BD')
        file_process_log_service = FileProcessLogService()
        tipo_str = self.get_tipo_str(self.tipo)
        error = file_process_log_service.log(self.request_origin,
                                        self.transaction_id,
                                        self.zip_name,
                                        tipo_str,
                                        input_url,
                                        output_url,
                                        self.get_errors(),
                                        self.get_logs(),
                                        self.user_id,
                                        self.selected_client)
        if error:
            self.track_monitoring(error)
            return False
        else:
            self.track_log(f'Registro efetuado')
            return True
    
    # 12. deleta chaves de controle
    def delete_keys_nf(self, transaction_id):        
        self.track_log(f'Deletando as chaves da transação {transaction_id}')
        
        try:
            self.robo_chaves_service.delete_by_transaction_id(transaction_id)
            self.track_log(f'Chaves deletadas')
        except Exception as ex:
            self.track_monitoring(f"Erro ao deletar chaves da transação - Erro: {str(ex)}")
            
    # 13. send email completo do log
    def send_email_logs(self):
        if DEBUG:
            return        
        # pega o tipo
        tipo_str = self.get_tipo_str(self.tipo)
        
        # envia e-mail do input (zip) e ouptut (excel) para o usuário
        body_html, body_text = self.nf_email_service.get_body_log(self.transaction_id, self.get_logs(), self.get_errors())
        subject = self.nf_email_service.get_subject_log(tipo_str, self.zip_name, self.request_origin)
            
        sucesso, code, msg = self.nf_email_service.send_email(self.user_email, self.zip_name, body_html, body_text, subject)
        self.track_log(msg)
        if not sucesso:
            self.track_error(msg)
        return sucesso
        # error, res = self.nf_email_service.send_email(self.user_email, self.zip_name, body_html, body_text, subject)
        # if error:
        #     return False
        # else:
        #     return True    
        
    # 14. salva xml no banco de dados
    def salvar_xml(self):
        if not self.ok:
            return
        
        self.track_log("Regsitrando dados dos XMLs no banco de dados")
                        
        erros, total_files, total_erros, \
        total_nf_bd, total_nf_view_bd = self.nf_xml_service.save_xmls(self.folder_extract_zip, self.selected_client)
        if total_nf_view_bd > 0:
            self.track_log(f"Registrado dados dos XMLs. Total XMLs: {total_files} Total NF registradas: {total_nf_bd} Total NCMs registrados: {total_nf_view_bd}")
        if erros:
            self.track_monitoring('Erros encontrados:')
            self.track_log(erros)
            self.track_error(erros)
            
    # 14. salva sefaz no banco de dados
    def salvar_sefaz(self, df):
        if not self.ok:
            return

        self.track_log("Registrando SEFAZ no banco de dados")
        status, msg = self.nf_excel_service.save_sefaz(df)

        if status:
            self.track_log(f"Registrado SEFAZ no banco de dados")
        else:
            self.track_monitoring(msg)
                                
    def get_s3_path(self, filename, folder: str = '', ext: str = ''):
        if not folder:
            folder = self.FOLDER_TEMP
        if ext:
            ext = f'.{ext}'
        filename = Path(filename).stem if ext else Path(filename).name  # Remove a extensão original do arquivo se uma nova extensão for fornecida, caso contrário, mantém o nome completo do arquivo.
        s3_path = f"{folder}/{filename}{ext}"
        return s3_path
        
    def get_tipo_str(self, tipo: int):
        if tipo == 1:
            return 'INSUMO'
        elif tipo == 2:
            return 'MILHO'
        elif tipo == 5:
            return 'CBIOS'
        elif tipo == 21:
            return 'DANFE'
        elif tipo == 22:
            return 'SEFAZ'
        elif tipo == 23:
            return 'CHAVES'
        return 'sem tipo'