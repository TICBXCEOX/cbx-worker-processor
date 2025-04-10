import os
import shutil

from os.path import join
from services.nf_processor_service import NotaFiscalProcessorService

class NotaFiscalService:
    def __init__(self):
        pass
    
    def unzip_file_and_process(self, userdata, s3_path_zip, zip_name, full_path_zip_filename, tipo, 
                               transaction_id, request_origin, selected_client=0):        
        nf_processor = NotaFiscalProcessorService()
        try:
            folder_extract_zip = join(os.path.dirname(full_path_zip_filename), 'extract')
            
            nf_processor.setup(request_origin, 
                            transaction_id, 
                            tipo,
                            zip_name, 
                            full_path_zip_filename, 
                            folder_extract_zip, 
                            selected_client, 
                            str(userdata['email']), 
                            userdata['user'][3]['message_group'], # message group para a fila - ex.: CBX
                            int(userdata['user'][0])) # user id
            
            if tipo not in [1, 2, 5, 21, 22, 23]:
                raise Exception("Tipo de processo inválido. Processo válidos: INSUMO, MILHO, CBIOS, DANFE, SEFAZ, CHAVES")
            
            # inicia processamento
            nf_processor.start()
            
            # descompacta zip
            nf_processor.unzip()
                       
            # processa arquivos
            if tipo == 1:
                df = nf_processor.processar_nfs_insumos()
            elif tipo == 2:
                df = nf_processor.processar_nfs_milho()
            elif tipo == 5:
                df = nf_processor.processar_nfs_cbios()
            elif tipo == 21:
                df = nf_processor.process_danfes()
            elif tipo == 22:
                df = nf_processor.processar_sefaz()
            elif tipo == 23:
                df = nf_processor.processar_chaves()
                            
            # define coluna chave nf
            key_nf_column = nf_processor.get_key_col(tipo)
            if tipo in [21, 22, 23]:
                df_sync = nf_processor.sync_key_nf(df, key_nf_column)
                df = nf_processor.filter_by_df_sync(df, df_sync, key_nf_column)                
            
            # upa zip s3
            #s3_path_zip = nf_processor.upload_zip_s3()
            
            # gera excel e upa s3
            s3_path_excel = nf_processor.upload_excel_nf_s3(df)
            
            # obtem urls s3
            input_url = nf_processor.get_input_url(s3_path_zip)
            output_url = nf_processor.get_output_url(s3_path_excel)
            
            # envia e-mail do processamento do arquivo
            nf_processor.send_email_process(input_url, output_url)
            
            # gera txt com as chaves e envia para fila
            if tipo in [21, 22, 23]:
                send = userdata['user'][3]['send_queue'] if 'send_queue' in userdata['user'][3] else False
                if send:
                    txt_url = nf_processor.generate_txt_chaves_s3(df, key_nf_column)
                    nf_processor.send_to_queue_robo(txt_url)
                
                if tipo == 22:
                    nf_processor.salvar_sefaz(df)
            else:
                # salva xml no banco de dados
                nf_processor.salvar_xml()
                
                # apaga as chaves quando não for DANFE ou SEFAZ            
                # as chaves na tabela de controle devem ser deletadas se o processamento não envolver DANFEs ou SEFAZ,
                # deve-se deletar as chaves associadas ao Transaction ID recebido, não há mais ncessidade de controle sobre elas
                nf_processor.delete_keys_nf(transaction_id)
                
            # registra processo no bd
            logou = nf_processor.log_process(input_url, output_url)
                                                                        
            # fim
            nf_processor.end()
            
            # envia email de log completo
            nf_processor.send_email_logs()
            
            return {
                "status": logou, 
                "erros": nf_processor.get_errors(),
                "logs": nf_processor.get_logs(),
                "total_files": nf_processor.total_files if nf_processor.total_files else 0,
                "filename": zip_name
            }
        except Exception as ex:            
            nf_processor.track_error(f"Erro Processamento: {str(ex)}")
            return {
                "status": False, 
                "erros": nf_processor.get_errors(), 
                "logs": None,
                "total_files": nf_processor.total_files if nf_processor and nf_processor.total_files else 0,
                "filename": zip_name
            }
        finally:
            if folder_extract_zip and os.path.exists(folder_extract_zip):  # Check if folder exists
                shutil.rmtree(folder_extract_zip)  # Delete folder and its contents

            if full_path_zip_filename and os.path.exists(full_path_zip_filename):  # Check if file exists
                os.remove(full_path_zip_filename)  # Delete the file