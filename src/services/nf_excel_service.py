import json
import unicodedata
import pandas as pd
from pathlib import Path
from configs import DEBUG
from services.nf_logger_service import NotaFiscalLoggerService
from services.sefaz_service import SefazService

class NotaFiscalExcelService:
    def __init__(self):
        self.nf_logger_service = NotaFiscalLoggerService()
            
    def save_sefaz(self, df: pd.DataFrame):
        bd = []
        for index, row in df.iterrows():
            properties = {
                'DATA_EMISSAO': str(row['DATA_EMISSAO']),
                'SERIE': row['SERIE'],
                'NUMERO_NF': row['NUMERO_NF'],
                'CHAVE_DE_ACESSO': row['CHAVE_DE_ACESSO'],
                'NATUREZA_OPERACAO': row['NATUREZA_OPERACAO'],
                'TIPO_EMISSAO': row['TIPO_EMISSAO'],
                'NUMR_PROTOCOLO': row['NUMR_PROTOCOLO'],
                'DATA_AUTORIZACAO': row['DATA_AUTORIZACAO'],
                'SITUACAO': row['SITUACAO'],
                'CNPJ_CPF_EMISSOR': row['CNPJ_CPF_EMISSOR'],
                'NOME_RAZAO_SOCIAL_EMISSOR': row['NOME_RAZAO_SOCIAL_EMISSOR'],
                'IE_EMISSOR': row['IE_EMISSOR'],
                'NOME_FANTASIA_EMISSOR': row['NOME_FANTASIA_EMISSOR'],
                'UF_EMISSOR': row['UF_EMISSOR'],
                'CNPJ_CPF_DESTINATARIO': row['CNPJ_CPF_DESTINATARIO'],
                'IE_DESTINATARIO': row['IE_DESTINATARIO'],
                'NOME_RAZAO_SOCIAL_DESTINATARIO': row['NOME_RAZAO_SOCIAL_DESTINATARIO'],
                'UF_DESTINATARIO': row['UF_DESTINATARIO'],
                'VALR_TOTAL_BASE_DE_CALCULO': row['VALR_TOTAL_BASE_DE_CALCULO'],
                'VALR_TOTAL_ICMS': row['VALR_TOTAL_ICMS'],
                'VALR_TOTAL_BC_ST': row['VALR_TOTAL_BC_ST'],
                'VALR_TOTAL_ICMS_ST': row['VALR_TOTAL_ICMS_ST'],
                'VALR_TOTAL_PRODUTO': row['VALR_TOTAL_PRODUTO'],
                'VALR_TOTAL_FRETE': row['VALR_TOTAL_FRETE'],
                'VALR_TOTAL_NOTA_FISCAL': row['VALR_TOTAL_NOTA_FISCAL'],
                'VALR_TOTAL_SERVICO': row['VALR_TOTAL_SERVICO']
            }
            
            nan_values = ['nan', 'NaN', 'Nan', "NAN"]
            if row['CNPJ_CPF_EMISSOR'] not in nan_values and row['CNPJ_CPF_DESTINATARIO'] not in nan_values:
                bd.append({'properties': json.dumps(properties),
                        'data_emissao': row['DATA_EMISSAO'],
                        'ie_emissor': row['IE_EMISSOR'],
                        'ie_destinatario': row['IE_DESTINATARIO'],
                        'cnpj_cpf_emissor': row['CNPJ_CPF_EMISSOR'],
                        'cnpj_cpf_destinatario': row['CNPJ_CPF_DESTINATARIO'],
                        'razao_social_emissor': row['NOME_RAZAO_SOCIAL_EMISSOR'],
                        'chave': row['CHAVE_DE_ACESSO'],
                        'situacao': row['SITUACAO']})

        sefaz_service = SefazService()
        status, msg = sefaz_service.insert_sefaz(bd)
        return status, msg

    def parser_sefaz(self, file_path):
        erros = []
        
        # file name with extension
        file_name = file_path.name 
        
        # Determine the correct engine based on file extension
        file_extension = file_path.suffix.lower()        
        engine = "xlrd" if file_extension == ".xls" else "openpyxl"

        # Read the Excel file without assuming headers
        df = pd.read_excel(file_path, dtype=str, engine=engine, header=None)                
        
        # Strip spaces and lowercase everything in the first column
        df.iloc[:, 0] = df.iloc[:, 0].str.strip().str.lower()

        # Find the first row where 'DATA EMISSÃO' or 'DATA EMISSAO' appears
        header_row_index = df[df.iloc[:, 0].str.contains(r"data emissão|data emissao", na=False, case=False, regex=True)].index
        if header_row_index.empty:
            raise ValueError(f"Coluna 'DATA EMISSÃO' não encontrada no arquivo Sefaz - {file_name}")
        header_row_index = header_row_index[0]  # Get the first match

        # Set this row as the header and keep only relevant data
        df = df.iloc[header_row_index:].reset_index(drop=True)

        # Set the first row as column names
        #df.columns = df.iloc[0].str.strip().str.lower()  # Normalize column names
        df = df[1:].reset_index(drop=True)  # Remove the first row from data
        
        # Apply to all column names
        #df.columns = [self.clean_column_name(col) for col in df.columns]
        
        # force the colum names
        df.columns = ['DATA_EMISSAO', 'SERIE', 'NUMERO_NF', 'CHAVE_DE_ACESSO', 'NATUREZA_OPERACAO', 
                      'TIPO_EMISSAO', 'NUMR_PROTOCOLO', 'DATA_AUTORIZACAO', 'SITUACAO', 
                      'CNPJ_CPF_EMISSOR', 'NOME_RAZAO_SOCIAL_EMISSOR', 'IE_EMISSOR', 'NOME_FANTASIA_EMISSOR', 'UF_EMISSOR', 
                      'CNPJ_CPF_DESTINATARIO', 'IE_DESTINATARIO', 'NOME_RAZAO_SOCIAL_DESTINATARIO', 'UF_DESTINATARIO',
                      'VALR_TOTAL_BASE_DE_CALCULO', 'VALR_TOTAL_ICMS', 'VALR_TOTAL_BC_ST',
                      'VALR_TOTAL_ICMS_ST', 'VALR_TOTAL_PRODUTO', 'VALR_TOTAL_FRETE',
                      'VALR_TOTAL_NOTA_FISCAL', 'VALR_TOTAL_SERVICO']                
        errors, total_after = self.format_column(df, file_name)
        if errors:
            erros.append(errors)
                                    
        return df, erros
    
    def processar_sefaz(self, pasta, filename):
        erros = []
        try:
            dados = []
            files = list(Path(pasta).rglob('*.[xX][lL][sS]')) + list(Path(pasta).rglob('*.[xX][lL][sS][xX]'))
           
            #for f in files:
            i = 0
            total = len(files)             
            while i < total:
                if DEBUG:
                    self.nf_logger_service.track_log(f'{i} de {total}')
                    
                f = files[i]
                if "__MACOSX" not in str(f):
                    try:
                        keys_nf, ers = self.parser_sefaz(f)
                        if keys_nf is not None and not keys_nf.empty:
                            dados.append(keys_nf)
                            if DEBUG:
                                self.nf_logger_service.track_log(f'arquivo {str(f)} - {len(keys_nf)} chaves')
                        if ers:
                            erros.extend(ers)
                    except Exception as ex:
                        erros.append(f"arquivo sefaz {str(f)} - erro: {str(ex)}")
                i += 1

            # Concatenate all the DataFrames into a single DataFrame
            df = pd.concat(dados, ignore_index=True)            
            
            if df.empty:
                erros.append(f"Sem Sefaz válidos para processar - Arquivo: {str(filename)}")
                return {
                    "status": False, 
                    "erros": erros, 
                    "total_files": len(files) if files else 0
                }
                                                                                                                           
            return {
                "status": True, 
                "erros": erros, 
                "total_files": len(files) if files else 0,
                "df": df
            }        
        except Exception as ex:
            erros.append(f"Erro ao processar SEFAZ. Arquivo: {str(filename)} - Erro: {str(ex)}")
            return {
                "status": False, 
                "erros": erros, 
                "total_files": len(files) if files else 0
            }
                
            
    def column_format_map(self):
        # Column format mapping
        column_formats = {
            'DATA_EMISSAO': 'datetime',
            'CNPJ_CPF_EMISSOR': 'cnpj_cpf',
            'CNPJ_CPF_DESTINATARIO': 'cnpj_cpf',
            'IE_DESTINATARIO': 'int',            
            'VALR_TOTAL_BASE_DE_CALCULO': 'number',
            'VALR_TOTAL_ICMS': 'number', 
            'VALR_TOTAL_BC_ST': 'number', 
            'VALR_TOTAL_ICMS_ST': 'number', 
            'VALR_TOTAL_PRODUTO': 'number',
            'VALR_TOTAL_FRETE': 'number', 
            'VALR_TOTAL_NOTA_FISCAL': 'number', 
            'VALR_TOTAL_SERVICO': 'number'
        }
        return column_formats        
    
    def format_column(self, df: pd.DataFrame, filename: str):
        erros = []
        total = len(df)
        total_after = total
        column_formats = self.column_format_map()
        
        replacemments = {"R$", "", ".", "", ",", ".", " ", ""}        

        for column, format_type in column_formats.items():
            if column in df.columns:
                if format_type == 'datetime':
                    df[column] = pd.to_datetime(df[column], format='%d/%m/%Y %H:%M:%S', errors='coerce')
                elif format_type == 'cnpj_cpf':
                    df[column] = df[column].str.replace(r"[./-]", "", regex=True)
                elif format_type == 'int':
                    df[column] = pd.to_numeric(df[column], errors='coerce', downcast='integer')
                elif format_type == 'number':
                    df[column] = df[column].replace(replacemments, regex=False)
                    df[column] = pd.to_numeric(df[column], errors='coerce', downcast='float')
        
        if all(col in df.columns for col in ['CNPJ_CPF_EMISSOR', 'CNPJ_CPF_DESTINATARIO']):
            df = df.dropna(subset=['CNPJ_CPF_EMISSOR', 'CNPJ_CPF_DESTINATARIO'], thresh=1)
            total_after = len(df)
            if total != total_after:
                erros.append('Foram removidas NFs com CPF/CNPJ Emissor e CPF/CNPJ Destinatário em branco')
                erros.append(f'Arquivo {filename}. Total: {total} Removidas: {total - total_after}')
        return erros, total_after