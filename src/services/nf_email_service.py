from services.email_service import EmailService
       
class NotaFiscalEmailService:
    def __init__(self):
        pass
    
    def get_tipo_str(self, tipo: int):
        if tipo == 1:
            return 'INSUMO'
        elif tipo == 2:
            return 'MILHO'
        elif tipo == 5:
            return 'CBIOS'
        elif tipo == 21:
            return 'DANFE'
        return 'sem tipo'   
        
    def get_subject_processing(self, tipo: str, file_name: str, request_origin: str):
        # Subject of the email
        tipo = f"{tipo}-" if tipo else ""
        origin = f"- {request_origin}" if request_origin else ""
        subject = f"Arquivo {tipo}{file_name} está pronto {origin}"
        return subject    
    
    def get_subject_no_processing(self, tipo: str, file_name: str, request_origin: str):
        # Subject of the email
        tipo = f"{tipo}-" if tipo else ""
        origin = f"- {request_origin}" if request_origin else ""
        subject = f"Arquivo {tipo}{file_name} NÃO processado {origin}"
        return subject    
    
    def get_subject_log(self, tipo: str, file_name: str, request_origin: str):
        # Subject of the email
        tipo = f"{tipo}-" if tipo else ""
        origin = f"- {request_origin}" if request_origin else ""
        subject = f"Arquivo {tipo}{file_name} - LOG Completo {origin}"
        return subject        
    
    def send_email(self, 
                to_address: str, 
                file_name: str,
                body_html: str, 
                body_text: str,                 
                subject: str):
        error: str = ''
        try:
            email_service = EmailService()
            sucesso, code, msg = email_service.just_send(email_service.from_address, to_address, '', subject, body_text, body_html)
            return sucesso, code, msg
        except Exception as ex:            
            error = f'Erro ao tentar enviar email para {to_address} do processamento do arquivo {file_name}. Erro: {str(ex)}'
            return False, 0, error
              
    def get_errors_html(self, errors):
        error_list = ("<ul>\n" + "\n".join(f"<li>{err}</li>" for err in errors) + "\n</ul>") if errors else ""

        error_html = f"""
            <div class="error-list">
            <p><strong>Erros Identificados:</strong></p>
            {error_list}
            </div>
        """ if errors else ""
        return error_html
    
    def get_logs_html(self, logs):
        log_list = ("<ul>\n" + "\n".join(f"<li>{log}</li>" for log in logs) + "\n</ul>") if logs else ""
        log_html = f"""
            <div class="log-list">
            <p><strong>Log do Processamento:</strong></p>
            {log_list}
            </div>
        """ if logs else ""
        return log_html
    
    def get_footer(self):
        html_content = f"""
        <footer style="font-family: Arial, sans-serif; color: #666; text-align: left; padding: 20px; border-top: 1px solid #ddd; margin-top: 20px;">
            <p style="font-size: 14px; margin: 0;">
                <strong>Equipe de TI</strong>
            </p>
            <p style="font-size: 12px; color: #999; margin: 5px 0;">
                CEOX Planejamento e Otimização
            </p>
            <p style="font-size: 12px; color: #999; margin: 5px 0;">
                Email: <a href="mailto:ti@cbxsustentabilidade.com.br" style="color: #1a73e8; text-decoration: none;">ti@cbxsustentabilidade.com.br</a>
            </p>
        </footer>
        """     
        return html_content         
                
    def get_body_processing(self,
                input_url: str, 
                output_url: str, 
                total_files: int,
                total_ok: int,
                total_errors: int,
                transaction_id: str,
                complement_body = ''):
        style = self.get_style()        
        footer = self.get_footer()
        
        inp = f'<li>📦 <a href="{input_url}" target="_blank">Baixar arquivo de input (ZIP)</a></li>' if input_url else '<li>Url do arquivo de input não foi gerada</li>'
        out = f'<li>📄 <a href="{output_url}" target="_blank">Baixar relatório detalhado (Excel)</a></li>' if output_url else '<li>Url do arquivo de output não foi gerada</li>'

        html_content = f'''
        <!DOCTYPE html>
        <html lang="pt-BR">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            {style}
        </head>
        <body>
            <div class="container">
                <h2>Relatório de Processamento do arquivo de Input</h2>
                <p>O arquivo ZIP foi processado com sucesso. Transaction ID: {transaction_id}</p>
                <p>Os resultados do processamento estão disponíveis para download:</p>
                <ul>
                    {inp}
                    {out}
                </ul>
                
                <div class="summary">
                    <p><strong>Total de arquivos:</strong>{total_files}</p>
                    <p><strong>Processados:</strong>{total_ok}</p>
                    <p><strong>Com erro:</strong>{total_errors}</p>
                </div>      
                
                {complement_body}
            </div>
        </body>
        {footer}
        </html>        
        '''     
        
        plain_text = f"Relatório de Processamento do arquivo de Input\n"
        plain_text += "O arquivo ZIP foi processado com sucesso.\n"
        plain_text = f"Transaction ID: {transaction_id}\n"
        plain_text += "Os resultados do processamento estão disponíveis para download:\n"
        plain_text += f"\n{input_url}\n"
        plain_text += f"\n{output_url}\n"
        plain_text += f"\nTotal de arquivos: {total_files}\n"
        plain_text += f"Processados: {total_ok}\n"
        plain_text += f"Com erro: {total_errors}\n"
        plain_text += f"\nQualquer dúvida, entre em contato\n"
        plain_text += f"\nEquipe de TI"        

        return html_content, plain_text

    def get_body_no_processing(self, input_url: str, transaction_id: str, errors = []):
        style = self.get_style()
        error_html = self.get_errors_html(errors)
        
        inp = f'<li>📦 <a href="{input_url}" target="_blank">Baixar arquivo de input (ZIP)</a></li>' if input_url else '<li>Url do arquivo de input não foi gerada</li>'
                        
        html_content = f'''
        <!DOCTYPE html>
        <html lang="pt-BR">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            {style}
        </head>
        <body>
            <div class="container">
                <h2>Nenhuma chave disponível para processamento.</h2>
                <p>O arquivo já foi processado anteriormente ou houve falha. Transaction ID: {transaction_id}</p>
                <ul>
                    {inp}
                </ul>
            </div>
            
            {error_html}
        </body>
        {self.get_footer()}
        </html>        
        '''
        
        plain_text = f"Nenhuma chave disponível para processamento.\n"
        plain_text = f"Transaction ID: {transaction_id}\n"        
        plain_text += "O arquivo já foi processado anteriormente ou houve falha.\n"
        plain_text += f"\n{input_url}\n"        
        plain_text += "\nERROS:\n" + "\n".join(errors) + "\n"
        plain_text += f"\nQualquer dúvida, entre em contato\n"
        plain_text += f"\nEquipe de TI"        
        
        return html_content, plain_text
    
    def get_body_log(self, transaction_id: str, logs = [], errors = []):
        style = self.get_style()
        log_html = self.get_logs_html(logs)
        error_html = self.get_errors_html(errors)
        footer = self.get_footer()
        
        html_content = f'''
        <!DOCTYPE html>
        <html lang="pt-BR">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            {style}
        </head>
        <body>
            <div class="container">
                <h2>Relatório Log COMPLETO do processamento do arquivo</h2>
                <p>Transaction ID: {transaction_id}</p>
                
                {log_html}
                
                {error_html}
            </div>
        </body>
        {footer}
        </html>        
        '''     
        
        plain_text = f"Relatório LOG completo do processamento do arquivo\n"
        plain_text = f"Transaction ID: {transaction_id}\n"
        plain_text += "\nLOGS:\n" + "\n".join(logs) + "\n"
        plain_text += "\nERROS:\n" + "\n".join(errors) + "\n"
        plain_text += f"\nQualquer dúvida, entre em contato\n"
        plain_text += f"\nEquipe de TI"

        return html_content, plain_text    

    def get_style(self):
        style = """
            <style>
                body {
                    font-family: Arial, sans-serif;
                    margin: 20px;
                    padding: 20px;
                    background-color: #f9f9f9;
                }
                .container {
                    max-width: 100%;
                    background: #ffffff;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.1);
                }
                h2 {
                    color: #333;
                }
                p {
                    font-size: 14px;
                    color: #555;
                }
                .summary {
                    margin: 15px 0;
                    padding: 10px;
                    background: #eef5ff;
                    border-left: 4px solid #007bff;
                }
                .error-list {
                    background: #ffecec;
                    padding: 10px;
                    border-left: 4px solid #e74c3c;
                    margin-top: 10px;
                }
                .log-list {
                    background: #E6F8E6;
                    padding: 10px;
                    border-left: 4px solid #2ecc71;
                    margin-top: 10px;
                }
                a {
                    color: #007bff;
                    text-decoration: none;
                    font-weight: bold;
                }
                a:hover {
                    text-decoration: underline;
                }
                ul {
                    padding-left: 20px;
                }
            </style>        
        """
        return style        