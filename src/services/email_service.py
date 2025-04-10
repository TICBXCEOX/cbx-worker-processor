# using SendGrid's Python Library
# https://github.com/sendgrid/sendgrid-python
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Cc, Content
from configs import *

class EmailService:
    def __init__(self) -> None:
        self.from_address = EMAIL_FROM
            
    def just_send(self, from_address, to_address, to_cc, subject, message_plain_text, message_html = ''):
        try:        
            from_email = Email(from_address)
                    
            # List of recipient email addresses
            to_addresses = [to_address]

            # Create a list of `To` objects for each recipient
            to_emails = [To(email) for email in to_addresses]        
                            
            mail = Mail(from_email, to_emails, subject, message_plain_text, message_html)

            if to_cc:
                cc_addresses = [to_cc]
                cc_emails = [Cc(email) for email in cc_addresses]            
                mail.cc = cc_emails
                
            sg = SendGridAPIClient(api_key=SENDGRID_API_KEY)
            response = sg.send(mail)
            return True, response.status_code, ''
        except Exception as e:
            error_message = f"Erro ao enviar email: {str(e)}"
            return False, 0, error_message
    
    def send_error(self, to_address, error_str, file_name, transaction_id):
        # Subject of the email
        subject = f'Erro ao gerar arquivo {file_name}'
        transaction_str = f'<p>Transaction ID: {transaction_id}</p>' if transaction_id else ''

        # Plain text version (fallback)
        plain_text = f"\nTrabsaction ID:{transaction_id}\nHouve um erro na geração e envio do arquivo de solicitado. Entre em contato com a Equipe de TI para maiores detalhes.\n\n\n{error_str}"

        # HTML version
        html_content = f"""
        <html>
            <body style="font-family: Arial, sans-serif; color: #666; text-align: left; padding: 20px; border-top: 1px solid #ddd; margin-top: 20px;">
                <h2 style="color:#333;">Erro do arquivo</h2>
                <p>Houve um erro na geração e envio do arquivo de solicitado.</p>
                {transaction_str}
                <p>Entre em contato com a Equipe de TI para maiores detalhes.</p>
                <br>                
                <p><strong>Erro reportado:</strong></p>
                <p>{error_str}</p>
            </body>
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
        </html>
        """
        
        sucess, code, msg = self.just_send(self.from_address, to_address, EMAIL_FROM, subject, plain_text, html_content)

        return sucess, code, msg
                            
    def get_flat_html_from_list(self, list):
        result = "<ul>\n" + "\n".join(f"<li>{f'{lst}'}</li>" for lst in list) + "\n</ul>" if list else ""
        return result
        
    def get_flat_str_from_list(self, list):
        result = "\n".join(list) if list else ""
        return result