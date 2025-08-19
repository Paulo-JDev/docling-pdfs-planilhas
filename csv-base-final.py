# Importa as bibliotecas necessárias
from docling.document_converter import DocumentConverter
from pathlib import Path
import pandas as pd

# Define o caminho para o arquivo PDF
pdf_path = Path("base/90013-2025-Termo-de-referencia.pdf")

# Inicia a conversão do documento
print("🔄 Iniciando a conversão do PDF...")
try:
    converter = DocumentConverter()
    # Converte o PDF em um objeto de documento Docling
    result_doc = converter.convert(str(pdf_path))
    print("✅ Conversão concluída!")
    
    # Verifica se há tabelas no documento e as extrai
    if result_doc.document.tables:
        print(f"🎉 {len(result_doc.document.tables)} tabela(s) encontrada(s)!")
        
        # Define os nomes de coluna desejados
        column_names = ['ITEM', 'ESPECIFICAÇÃO', 'CATMAT', 'UF', 'QTD', 'VALOR UNITÁRIO', 'VALOR TOTAL']
        
        # Itera sobre cada tabela encontrada
        for idx, table in enumerate(result_doc.document.tables):
            print("-" * 50)
            print(f"Conteúdo da Tabela {idx + 1}:")
            
            # Exporta a tabela para um DataFrame do pandas
            df = table.export_to_dataframe()
            
            # Renomeia as colunas para o padrão desejado, se o número de colunas for o mesmo
            if len(df.columns) == len(column_names):
                df.columns = column_names
            else:
                print(f"Aviso: O número de colunas da Tabela {idx + 1} ({len(df.columns)}) não corresponde ao padrão esperado ({len(column_names)}). O cabeçalho não será renomeado.")
            
            # Exibe o DataFrame
            print(df)
            
            # Salva em um arquivo CSV com o cabeçalho padronizado
            # Garante que os arquivos sejam salvos na pasta 'base' para manter a organização
            output_csv_path = Path("base") / f"tabela_{idx + 1}.csv"
            df.to_csv(output_csv_path, index=False, encoding='utf-8')
            print(f"Tabela {idx + 1} salva como '{output_csv_path}' com cabeçalho padronizado.")
    else:
        print("❌ Nenhuma tabela foi encontrada neste documento PDF.")
        
except FileNotFoundError:
    print(f"❌ Erro: O arquivo '{pdf_path}' não foi encontrado. Por favor, verifique o caminho do arquivo.")
except Exception as e:
    print(f"❌ Ocorreu um erro ao processar o arquivo: {e}")