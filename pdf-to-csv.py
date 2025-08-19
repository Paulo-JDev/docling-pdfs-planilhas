# -*- coding: utf-8 -*-

"""
Script unificado para extrair tabelas de um documento PDF,
processar os dados em memória e salvar em um único arquivo CSV consolidado.
"""

# Importa as bibliotecas necessárias
from docling.document_converter import DocumentConverter
from pathlib import Path
import pandas as pd
import re

# --- CONFIGURAÇÕES ---
# Defina os caminhos de entrada e saída aqui para fácil manutenção
PDF_INPUT_PATH = Path("base/90013-2025-Termo-de-referencia.pdf")
CSV_OUTPUT_PATH = Path("base/planilha_consolidada_final.csv")
# ---------------------

def extract_tables_from_pdf(pdf_path: Path) -> list:
    """
    Extrai todas as tabelas de um arquivo PDF e as retorna como uma lista de DataFrames do pandas.

    Args:
        pdf_path (Path): O caminho para o arquivo PDF.

    Returns:
        list: Uma lista de DataFrames, onde cada DataFrame representa uma tabela extraída.
              Retorna uma lista vazia se nenhuma tabela for encontrada ou em caso de erro.
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
        
        # Converte todas as tabelas encontradas para DataFrames e retorna a lista
        list_of_dfs = [table.export_to_dataframe() for table in result_doc.document.tables]
        return list_of_dfs

    except Exception as e:
        print(f"❌ Ocorreu um erro ao processar o arquivo PDF: {e}")
        return []

def extract_short_description(text: str) -> str:
    """
    Extrai a parte inicial do texto até o primeiro ponto ou vírgula.

    Args:
        text (str): O texto completo da especificação.

    Returns:
        str: A descrição curta extraída.
    """
    text = str(text).strip()
    if not text:
        return ""
    
    # Busca por qualquer caractere que não seja ponto ou vírgula no início da string
    match = re.match(r"([^.,]+)", text)
    return match.group(1).strip() if match else ""

def process_and_consolidate(list_of_dfs: list) -> pd.DataFrame:
    """
    Processa uma lista de DataFrames, limpa os dados, extrai as informações
    relevantes e consolida tudo em um único DataFrame.

    Args:
        list_of_dfs (list): A lista de DataFrames brutos extraídos do PDF.

    Returns:
        pd.DataFrame: Um DataFrame consolidado e limpo, ou um DataFrame vazio se não houver dados.
    """
    all_processed_data = []
    print("\n🔄 Iniciando o processamento e a consolidação das tabelas...")

    for i, df_raw in enumerate(list_of_dfs):
        print(f"--- Processando Tabela {i + 1} ---")
        try:
            # Faz uma cópia para evitar modificar o original durante a iteração
            df = df_raw.copy()

            # Lógica para encontrar a linha do cabeçalho dinamicamente
            header_row_index = -1
            for idx in range(min(len(df), 10)): # Procura nas primeiras 10 linhas
                row_values = [str(val).strip().upper() for val in df.iloc[idx].tolist()]
                if 'ITEM' in row_values:
                    header_row_index = idx
                    break
            
            # Se o cabeçalho foi encontrado, redefine o DataFrame
            if header_row_index != -1:
                df.columns = df.iloc[header_row_index]
                df = df.drop(df.index[:header_row_index + 1]).reset_index(drop=True)
            else:
                # Se não encontrar 'ITEM', tenta usar a primeira linha como cabeçalho
                # e renomear colunas com base na posição
                print(f"Aviso: Cabeçalho 'ITEM' não encontrado na Tabela {i + 1}. Tentando renomear por posição.")
                df.columns = [f'col_{j}' for j in range(len(df.columns))]
                # Mapeia as colunas esperadas pela posição
                column_map = {'col_0': 'ITEM', 'col_1': 'ESPECIFICAÇÃO', 'col_2': 'CATMAT'}
                df.rename(columns=column_map, inplace=True)

            # Define as colunas de interesse
            required_cols = ['ITEM', 'ESPECIFICAÇÃO', 'CATMAT']
            if not all(col in df.columns for col in required_cols):
                print(f"Aviso: A Tabela {i + 1} não contém todas as colunas necessárias ({', '.join(required_cols)}). Pulando...")
                continue

            # Itera sobre as linhas do DataFrame limpo
            for _, row in df.iterrows():
                item_val = str(row.get('ITEM', '')).strip()
                especificacao_val = str(row.get('ESPECIFICAÇÃO', '')).strip()
                catalogo_val = str(row.get('CATMAT', '')).strip()

                # Filtra linhas vazias ou que ainda parecem ser cabeçalhos
                if not item_val or item_val.lower() == 'nan' or not especificacao_val or especificacao_val.lower() == 'nan':
                    continue

                # Cria a descrição curta
                descricao_curta = extract_short_description(especificacao_val)
                
                # Adiciona os dados processados à lista
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


def main():
    """
    Função principal que orquestra o fluxo de trabalho completo.
    """
    # 1. Extrai as tabelas do PDF para uma lista de DataFrames
    extracted_dfs = extract_tables_from_pdf(PDF_INPUT_PATH)

    # 2. Se alguma tabela foi extraída, processa e consolida os dados
    if extracted_dfs:
        final_df = process_and_consolidate(extracted_dfs)

        # 3. Se o DataFrame final não estiver vazio, salva em CSV
        if not final_df.empty:
            print("\n✅ Processamento concluído!")
            try:
                final_df.to_csv(CSV_OUTPUT_PATH, index=False, encoding='utf-8-sig') # utf-8-sig para melhor compatibilidade com Excel
                print(f"🎉 Planilha consolidada foi salva com sucesso em: '{CSV_OUTPUT_PATH}'")
                print("\nVisualizando as 10 primeiras linhas do resultado final:")
                print(final_df.head(10))
            except Exception as e:
                print(f"❌ Erro ao salvar o arquivo CSV final: {e}")
        else:
            print("\n❌ Nenhum dado válido foi extraído das tabelas para gerar a planilha final.")
    else:
        print("\nProcesso encerrado pois nenhuma tabela foi extraída do PDF.")


# Ponto de entrada do script: executa a função main
if __name__ == "__main__":
    main()