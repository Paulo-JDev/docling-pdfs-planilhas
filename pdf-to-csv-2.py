# -*- coding: utf-8 -*-

"""
Script unificado para extrair tabelas de um documento PDF,
processar os dados em memória e salvar em um único arquivo CSV consolidado.
O usuário pode selecionar o arquivo PDF através de uma interface gráfica.
"""

# Importa as bibliotecas necessárias
from docling.document_converter import DocumentConverter
from pathlib import Path
import pandas as pd
import re

# Novas importações para o seletor de arquivos
import tkinter as tk
from tkinter import filedialog

# --- (As funções extract_tables_from_pdf, extract_short_description e process_and_consolidate continuam exatamente as mesmas de antes) ---

def extract_tables_from_pdf(pdf_path: Path) -> list:
    """
    Extrai todas as tabelas de um arquivo PDF e as retorna como uma lista de DataFrames do pandas.
    (Esta função não muda)
    """
    print(f"🔄 Iniciando a conversão do PDF: '{pdf_path}'...")
    if not pdf_path.exists():
        print(f"❌ Erro Crítico: O arquivo '{pdf_path}' não foi encontrado.")
        return []

    try:
        converter = DocumentConverter()
        result_doc = converter.convert(str(pdf_path))
        print("✅ Conversão concluída!")

        if not result_doc.document.tables:
            print("❌ Nenhuma tabela foi encontrada neste documento PDF.")
            return []

        print(f"🎉 {len(result_doc.document.tables)} tabela(s) encontrada(s)!")
        
        list_of_dfs = [table.export_to_dataframe() for table in result_doc.document.tables]
        return list_of_dfs

    except Exception as e:
        print(f"❌ Ocorreu um erro ao processar o arquivo PDF: {e}")
        return []

def extract_short_description(text: str) -> str:
    """
    Extrai a parte inicial do texto até o primeiro ponto ou vírgula.
    (Esta função não muda)
    """
    text = str(text).strip()
    if not text:
        return ""
    
    match = re.match(r"([^.,]+)", text)
    return match.group(1).strip() if match else ""

def process_and_consolidate(list_of_dfs: list) -> pd.DataFrame:
    """
    Processa uma lista de DataFrames, limpa os dados, extrai as informações
    relevantes e consolida tudo em um único DataFrame.
    (Esta função não muda)
    """
    all_processed_data = []
    print("\n🔄 Iniciando o processamento e a consolidação das tabelas...")

    for i, df_raw in enumerate(list_of_dfs):
        print(f"--- Processando Tabela {i + 1} ---")
        try:
            df = df_raw.copy()

            header_row_index = -1
            for idx in range(min(len(df), 10)):
                row_values = [str(val).strip().upper() for val in df.iloc[idx].tolist()]
                if 'ITEM' in row_values:
                    header_row_index = idx
                    break
            
            if header_row_index != -1:
                df.columns = df.iloc[header_row_index]
                df = df.drop(df.index[:header_row_index + 1]).reset_index(drop=True)
            else:
                print(f"Aviso: Cabeçalho 'ITEM' não encontrado na Tabela {i + 1}. Tentando renomear por posição.")
                df.columns = [f'col_{j}' for j in range(len(df.columns))]
                column_map = {'col_0': 'ITEM', 'col_1': 'ESPECIFICAÇÃO', 'col_2': 'CATMAT'}
                df.rename(columns=column_map, inplace=True)

            required_cols = ['ITEM', 'ESPECIFICAÇÃO', 'CATMAT']
            if not all(col in df.columns for col in required_cols):
                print(f"Aviso: A Tabela {i + 1} não contém todas as colunas necessárias ({', '.join(required_cols)}). Pulando...")
                continue

            for _, row in df.iterrows():
                item_val = str(row.get('ITEM', '')).strip()
                especificacao_val = str(row.get('ESPECIFICAÇÃO', '')).strip()
                catalogo_val = str(row.get('CATMAT', '')).strip()

                if not item_val or item_val.lower() == 'nan' or not especificacao_val or especificacao_val.lower() == 'nan':
                    continue

                descricao_curta = extract_short_description(especificacao_val)
                
                processed_row = {
                    "item": item_val,
                    "catalogo": catalogo_val,
                    "descricao": descricao_curta,
                    "descricao_detalhada": especificacao_val
                }
                all_processed_data.append(processed_row)
        
        except Exception as e:
            print(f"Erro ao processar a Tabela {i + 1}: {e}")

    if not all_processed_data:
        return pd.DataFrame()

    return pd.DataFrame(all_processed_data)

# --- FUNÇÃO MAIN MODIFICADA ---
def main():
    """
    Função principal que orquestra o fluxo de trabalho completo.
    """
    # Cria uma janela raiz do Tkinter mas a esconde
    root = tk.Tk()
    root.withdraw()

    print("Por favor, selecione o arquivo PDF na janela que apareceu...")
    
    # Abre a janela de diálogo para selecionar um arquivo PDF
    pdf_path_str = filedialog.askopenfilename(
        title="Selecione o arquivo PDF para processar",
        filetypes=[("Arquivos PDF", "*.pdf")]
    )

    # Se o usuário cancelar a seleção, o caminho será vazio
    if not pdf_path_str:
        print("\nNenhum arquivo selecionado. Encerrando o programa.")
        return

    # Converte o caminho para um objeto Path
    pdf_input_path = Path(pdf_path_str)
    
    # Gera o nome do arquivo de saída baseado no de entrada
    # Ex: "meu_arquivo.pdf" -> "base/meu_arquivo_consolidada.csv"
    output_folder = Path("base")
    output_folder.mkdir(exist_ok=True) # Garante que a pasta 'base' exista
    csv_output_path = output_folder / f"{pdf_input_path.stem}_consolidada.csv"

    # 1. Extrai as tabelas do PDF
    extracted_dfs = extract_tables_from_pdf(pdf_input_path)

    # 2. Processa e consolida
    if extracted_dfs:
        final_df = process_and_consolidate(extracted_dfs)

        # 3. Salva o resultado
        if not final_df.empty:
            print("\n✅ Processamento concluído!")
            try:
                final_df.to_csv(csv_output_path, index=False, encoding='utf-8-sig')
                print(f"🎉 Planilha consolidada foi salva com sucesso em: '{csv_output_path}'")
                print("\nVisualizando as 10 primeiras linhas do resultado final:")
                print(final_df.head(10))
            except Exception as e:
                print(f"❌ Erro ao salvar o arquivo CSV final: {e}")
        else:
            print("\n❌ Nenhum dado válido foi extraído das tabelas para gerar a planilha final.")
    else:
        print("\nProcesso encerrado pois nenhuma tabela foi extraída do PDF.")

# Ponto de entrada do script
if __name__ == "__main__":
    main()