import pandas as pd
import glob
import re
from pathlib import Path

# Pasta onde os arquivos CSV estão localizados
data_folder = Path("base")

def extract_descricao(text):
    """
    Extrai a parte inicial do texto, até o primeiro ponto ou vírgula.
    Converte para string para evitar erros de tipo e garantir o tratamento de caracteres.
    """
    text = str(text).strip()
    # Verifica se a string tem algum conteúdo antes de tentar o regex
    if not text:
        return ""
        
    match = re.match(r"([^.,]+)", text)
    if match:
        return match.group(1).strip()
    return ""

def process_all_csvs(folder_path):
    """
    Encontra, ordena e processa todos os arquivos CSV em uma pasta,
    consolidando-os em um único DataFrame com os itens originais do CSV.
    """
    all_data = []
    
    # Encontra todos os arquivos CSV e ordena-os numericamente para processamento correto
    csv_files = glob.glob(str(folder_path / "*.csv"))
    
    def sort_key(file_path):
        match = re.search(r'tabela_(\d+)\.csv', file_path)
        return int(match.group(1)) if match else 0
        
    sorted_files = sorted(csv_files, key=sort_key)
    
    if not sorted_files:
        print(f"Nenhum arquivo CSV encontrado na pasta '{folder_path}'.")
        return
        
    for file in sorted_files:
        try:
            # Tenta ler o CSV sem cabeçalho primeiro para inspecionar
            df = pd.read_csv(file, header=None, encoding='utf-8')
            
            # Determina qual linha é o cabeçalho real e qual o tipo (nomes vs. numérico)
            header_row_index = -1
            header_type = None

            # Itera sobre as primeiras 20 linhas para encontrar 'ITEM'
            for i in range(min(len(df), 20)):
                row_as_list = [str(x).strip().upper() for x in df.iloc[i].dropna()]
                if 'ITEM' in row_as_list:
                    header_row_index = i
                    header_type = 'names'
                    break
                # Se não encontrar 'ITEM', verifica se é o cabeçalho numérico '0,1,2,3...'
                elif len(row_as_list) > 2 and row_as_list[0] == '0' and row_as_list[1] == '1' and row_as_list[2] == '2':
                    header_row_index = i
                    header_type = 'numeric'
                    break

            # Se encontrar o cabeçalho, lê novamente com a configuração correta
            if header_type == 'names':
                df = pd.read_csv(file, header=header_row_index, encoding='utf-8')
                item_col_name = 'ITEM'
                especificacao_col_name = 'ESPECIFICAÇÃO'
                catalogo_col_name = 'CATMAT'
            elif header_type == 'numeric':
                df = pd.read_csv(file, header=header_row_index, encoding='utf-8')
                # Mapeia as colunas numéricas para os nomes desejados
                df.rename(columns={0: 'ITEM', 1: 'ESPECIFICAÇÃO', 2: 'CATMAT'}, inplace=True)
                item_col_name = 'ITEM'
                especificacao_col_name = 'ESPECIFICAÇÃO'
                catalogo_col_name = 'CATMAT'
            else:
                print(f"Aviso: Não foi possível identificar o cabeçalho no arquivo {file}. Pulando...")
                continue

            # Itera sobre as linhas do DataFrame para processar os dados
            for index, row in df.iterrows():
                try:
                    # Usa os nomes das colunas mapeadas para garantir a consistência
                    item_val = str(row[item_col_name]).strip()
                    especificacao_val = str(row[especificacao_col_name]).strip()
                    catalogo_val = str(row[catalogo_col_name]).strip()
                except KeyError:
                    # Se a linha for inválida (ex: cabeçalho duplicado), ignora
                    continue

                # Remove linhas vazias ou de cabeçalho que foram lidas como dados
                if not item_val or item_val.lower() in ['nan', 'item', '0'] or \
                   not especificacao_val or especificacao_val.lower() in ['nan', 'especificação', '1']:
                    continue

                descricao_curta = extract_descricao(especificacao_val)
                
                processed_row = {
                    "item": item_val,
                    "catalogo": catalogo_val,
                    "descricao": descricao_curta,
                    "descricao_detalhada": especificacao_val
                }
                all_data.append(processed_row)
        
        except Exception as e:
            print(f"Erro ao processar o arquivo {file}: {e}")

    # Cria o DataFrame final
    if all_data:
        final_df = pd.DataFrame(all_data)
        
        # Salva o DataFrame final em um novo arquivo CSV na pasta 'csv'
        output_filename = folder_path / "planilha_consolidada.csv"
        final_df.to_csv(output_filename, index=False, encoding='utf-8')
        
        print(f"\n🎉 Processamento concluído! A planilha consolidada foi salva em '{output_filename}'.")
        print("\nVisualizando as primeiras linhas da nova planilha:")
        print(final_df.head(10)) # Exibe as 10 primeiras linhas para melhor visualização
    else:
        print("\nNenhum dado válido foi extraído dos arquivos CSV.")

# Executa a função principal
if not data_folder.exists():
    print(f"Erro: A pasta '{data_folder}' não foi encontrada.")
else:
    process_all_csvs(data_folder)