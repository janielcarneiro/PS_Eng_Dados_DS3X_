# PS_Eng_Dados_DS3X_

<p align="center">
  <img src="https://github.com/user-attachments/assets/2c3be923-78e1-4fcb-bb8d-66da3ad56fb7" alt="image"/>
</p>


# Primeiro Passo

Baixe o projeto, crie uma pasta (credentials dentro  da pasta principal é adicione as credencias do google cloud).
Não coloque as minhas porque o Git identica é não deixa subir por motivos de segurança.

# Segundo passo

instale todas as depedencias no seu ambiente virtual python 

COMNADOS:<br>
pip install selenium <br>
pip install webdriver-manager<br>
pip install pandas<br>
pip install google-cloud-bigquery<br>
pip install selenium webdriver-manager pandas google-cloud-bigquery<br>

# Terceiro passo 
Temos apenas um arquivo app.py com os algoritmos

execute primeiro esse trecho de codigo é comente o restante abaixo, que esta abaixo dele:
```import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service


import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery

# Diretório para salvar os downloads
download_dir = os.path.abspath("/home/janiel/Documentos/docker_test/downloads")
os.makedirs(download_dir, exist_ok=True)

# Essa parte vai configurar o chrome
service = Service(ChromeDriverManager().install())
options = webdriver.ChromeOptions()
options.add_argument("--no-sandbox")  # Evita problemas em ambientes restritos como Docker
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--remote-debugging-port=9222") 

# Configurando á pasta para downloads
prefs = {
    "download.default_directory": download_dir,  # Caminho para a pasta de downloads
    "download.prompt_for_download": False,  # Evita o prompt de download
    "download.directory_upgrade": True,  # Permite substituir diretórios
}
options.add_experimental_option("prefs", prefs)

# Inicializa o driver
driver = webdriver.Chrome(service=service, options=options)

# URLs das páginas
urls = {
    "icc": "https://www.fecomercio.com.br/pesquisas/indice/icc",  
    "icf": "https://www.fecomercio.com.br/pesquisas/indice/icf"  
}
file_names = {
    "icc": "icc.xlsx",
    "icf": "icf.xlsx"
}

try:
    for name, url in urls.items():
        print(f"Acessando {url}...")
        driver.get(url)  # Acessa a página
        time.sleep(5)  # Aguardar o carregamento 

        # Localiza o botão de download
        try:
            download_button = driver.find_element(By.XPATH, '//a[contains(@class, "download")]')
            # Clica no botão
            ActionChains(driver).move_to_element(download_button).click().perform()
            print(f"Baixando arquivo para {name}...")
        except Exception as e:
            print(f"Erro ao localizar ou clicar no botão de download em {url}: {e}")
        
        # Aguarda terminar
        time.sleep(10)

        # ver todos os arquivos baixados
        downloaded_files = os.listdir(download_dir)
        print("Arquivos baixados:", downloaded_files)  # Imprime os arquivos na pasta de downloads

        # Renomeia os arquivos
        for file in downloaded_files:
            if file.endswith(".xlsx"):
                print(f"Renomeando o arquivo {file}...")
                # Renomeia com base no nome correto
                old_path = os.path.join(download_dir, file)
                new_path = os.path.join(download_dir, file_names[name])
                os.rename(old_path, new_path)
                print(f"Arquivo {file} renomeado para {file_names[name]}")
                break

finally:
    # dechar a operação do navegador
    driver.quit()

print(f"Arquivos baixados em: {download_dir}")


```
# Quarto
 PARA QUE ESSE ALGORITMO FUNCIONE VOCÊ PRECISSA ABRIR CADA ARQUIVO
 E RENOMEAR AS DATAS DELE EX: DEZ-23, JAN-24 COLOQUE DEZEMBRO_23, JANEIRO_24 ETC...
 POIS NÃO FOI FEITO O ALGORITMO RENOMEAR ALTOMATICAMENTE (DEVIDO AO TEMPO)

# Quinto

  Comente o codigo acima é tire o comentario desse codigo é execute ele:

  ``` # Configurar credenciais do Google Cloud
credentials_path = "/home/janiel/Documentos/docker_test/credentials/SA-janielc895.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Inicializar cliente BigQuery
client = bigquery.Client()

# Verificar projeto
project_id = client.project
dataset_id = "janielc895" 
print(f"ID do projeto autenticado: {project_id}")

# carregar um DataFrame ajustado no BigQuery
def upload_to_bigquery(df, table_name):
    print(f"Iniciando o carregamento de dados para a tabela {table_name}...")

    try:
        # adicionei um tempo no carregamento
        df["load_timestamp"] = datetime.now(timezone.utc)

        # Configuração da tabela no BigQuery
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # Sobrescrever dados existentes
            source_format=bigquery.SourceFormat.CSV,  # Formato esperado
            autodetect=True  # Detectar tipos de colunas automaticamente
        )

        # Carregar dados no BigQuery
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # esperar terminar

        # ver como está
        table = client.get_table(table_id)
        print(f"Carregamento concluído. Tabela {table_id} contém {table.num_rows} linhas.")

    except Exception as e:
        print(f"Erro ao carregar para o BigQuery: {e}")

# Essa função vai enviar os dados
def process_and_upload(file_path, table_name):
    try:
        # Lê o arquivo Excel
        df = pd.read_excel(file_path, skiprows=1)  # Ajuste se houver cabeçalhos extras

        # Remove linhas e colunas completamente vazias
        df = df.dropna(how="all", axis=0)
        df = df.dropna(how="all", axis=1)

        # Ajusta os nomes das colunas
        df.columns = (
            df.columns
            .str.replace(r"[^\w\s]", "_", regex=True)  # Substitui caracteres especiais por "_"
            .str.strip()  # Remove espaços extras nas bordas
            .str.replace(r"\s+", "_", regex=True)  # Substitui múltiplos espaços por "_"
            .str.lower()  # Deixa os nomes em minúsculas
        )

        # Renomeia a coluna "unnamed__0" para "categoria", se existir
        if "unnamed__0" in df.columns:
            df = df.rename(columns={"unnamed__0": "categoria"})

        # Itera pelas colunas para tentar converter as que aparentam ser datas
        for col in df.columns:
            if pd.api.types.is_object_dtype(df[col]):
                # Tenta converter os valores em datas no formato "mês-ano" explícito
                try:
                    df[col] = pd.to_datetime(
                        df[col], 
                        format="%b-%y",  
                        errors="ignore"  # Não forçará a conversão se o formato não corresponder
                    )
                except Exception as e:
                    print(f"Erro ao processar a coluna '{col}': {e}")

        # Multiplica apenas as últimas duas colunas numéricas por 100 para os dados ficar correto
        numeric_cols = df.select_dtypes(include="number").columns[-2:]  # Seleciona as últimas duas colunas numéricas
        for col in numeric_cols:
            df[col] = df[col] * 100  # Multiplica os valores por 100

        # Exibe os nomes ajustados das colunas
        print("\nNomes das colunas ajustados:")
        print(df.columns.tolist())

        # Exibe os dados ajustados
        print("\nDados ajustados:")
        print(df.head(20))

        # Envia os dados para o BigQuery
        upload_to_bigquery(df, table_name)

    except FileNotFoundError:
        print(f"Arquivo '{file_path}' não encontrado. Verifique o caminho.")
    except Exception as e:
        print(f"Ocorreu um erro ao tentar ler o arquivo: {e}")

# Caminhos dos arquivos
file_paths = [
    "/home/janiel/Documentos/docker_test/downloads/icc.xlsx",  # Caminho do arquivo ICC
    "/home/janiel/Documentos/docker_test/downloads/icf.xlsx"   # Caminho do arquivo ICF
]

# Processa e envia os dados de ambos os arquivos
for file_path in file_paths:
    table_name = "icc_raw" if "icc" in file_path else "icf_raw"
    process_and_upload(file_path, table_name)


 ```
# Melhorias Fara projeto <br>
 1 - Dividir o codigo em arquivos diferentes com função <br>
 2 - criar uma função pra renomear o nome dos arquivos automatico <br>


