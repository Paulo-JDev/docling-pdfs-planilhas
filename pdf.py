# Importa as bibliotecas necessárias
from docling.document_converter import DocumentConverter
from pathlib import Path
import pandas as pd

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
        
        # Itera sobre cada tabela encontrada
        for idx, table in enumerate(result_doc.document.tables):
            print("-" * 50)
            print(f"Conteúdo da Tabela {idx + 1}:")
            
            # Exporta a tabela para um DataFrame do pandas
            df = table.export_to_dataframe()
            
            # Exibe o DataFrame
            print(df)
            
            # Opcional: Para salvar em um arquivo CSV, descomente as linhas abaixo
            df.to_csv(f"tabela_{idx + 1}.csv", index=False)
            print(f"Tabela {idx + 1} salva como 'tabela_{idx + 1}.csv'")
    else:
        print("❌ Nenhuma tabela foi encontrada neste documento PDF.")
        
except FileNotFoundError:
    print(f"❌ Erro: O arquivo '{pdf_path}' não foi encontrado. Por favor, verifique o caminho do arquivo.")
except Exception as e:
    print(f"❌ Ocorreu um erro ao processar o arquivo: {e}")