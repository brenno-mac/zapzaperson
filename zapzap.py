from whatsapp_api_client_python import API
import pandas as pd
from datetime import date
from google.cloud import bigquery
import os

#auth-bq
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/brenno/git/ZAP/googlecredentials.json'
project_id = 'datalake-2022'
client = bigquery.Client()

#auth#
IdInstance = '7103841652'
ApiTokenInstance = 'e79b6f46b7bd477fa02da78537c2608dfc26e14722b7462a8c'
greenAPI = API.GreenApi(IdInstance, ApiTokenInstance)

#TEMPO#
currenttime = date.today()

#babilaques#
configinsta = "SELECT * FROM `datalake-2022.comunidades_whatsapp.utils`"
configg = pd.read_gbq(configinsta, project_id = project_id, dialect = 'standard')

# msg_receb = greenAPI.journals.lastIncomingMessages()
# df = pd.DataFrame(msg_receb.data)
# conversas = df['chatId'].unique()
# filtrado = [item for item in conversas if item.endswith("@g.us")]
nome_grupos = []
for i in configg['groupId']:
  a = greenAPI.groups.getGroupData(i)
  nome_grupos.append(a.data)
info_grupos_df = pd.DataFrame(nome_grupos)


def tblpessoas(*args):
  pessoas = pd.DataFrame()
  for arg in args:
    indice = info_grupos_df[info_grupos_df['subject'] == arg].index[0]
    participantes_dict = nome_grupos[indice]['participants']
    participantes_id = pd.DataFrame(participantes_dict)
    participantes_id = participantes_id['id']
    lista_participantes = []
    for i in participantes_id:
      a = greenAPI.serviceMethods.getContactInfo(i).data
      lista_participantes.append(a)
    
    lista_participantes = pd.DataFrame(lista_participantes)
    participantes = {
      'id_participantes' : lista_participantes['chatId'],
      'nomes_participantes' : lista_participantes['name']
    } 
  
    participantes = pd.DataFrame.from_dict(participantes, orient='index').T
    participantes['grupo'] = arg
    
    pessoas = pd.concat([pessoas, participantes])
    pessoas.drop(['grupo'], axis=1, inplace=True)
    pessoas.drop_duplicates(inplace=True, subset = 'id_participantes')
    pessoas.reset_index(drop=True,inplace=True)
    pessoas['id'] = pessoas.index
    pessoas['numero'] = pessoas['id_participantes'].str.split('@').str[0]
    pessoas = pessoas[['id', 'id_participantes', 'nomes_participantes', 'numero']]
    pessoas['date_etl'] = currenttime
  return pessoas

#tabela de pessoas - tabela única, sem registro histórico#
pessoas = tblpessoas('#EcossistemaFIS','Mulheres na Saúde FIS', 'RJ by #FIS', 'Saúde Digital, Medicina Conectada & Novas Tecnologias')
pessoas
query = """
SELECT * FROM `datalake-2022.comunidades_whatsapp.pessoas` 
"""
getpessoas = pd.read_gbq(query,project_id=project_id )
pessoas = pd.concat([getpessoas,pessoas])
pessoas.drop_duplicates(subset = 'id_participantes', inplace=True)
pessoas.reset_index(drop=True, inplace=True)
pessoas.drop(['id'], axis=1, inplace=True)
pessoas['id'] = pessoas.index
pessoas = pessoas[['id', 'id_participantes', 'nomes_participantes', 'numero']]
pessoas.rename(columns={"id_participantes" : "id_api", "nomes_participantes" : "nome"}, inplace=True)
pessoas['date_etl'] = currenttime
job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
job = client.load_table_from_dataframe(pessoas, 'comunidades_whatsapp.grupos',job_config=job_config)
  
def achei(*args):
  # pessoas.drop(['nome', 'numero'], axis=1, inplace=True)
  # pessoas = pd.DataFrame()
  for arg in args:
    pessoas[arg] = pessoas['id_api'].apply(lambda x: 1 if any(d['id'] == x for d in info_grupos_df['participants'][info_grupos_df[info_grupos_df['subject'] == arg].index[0]]) else 0)
  return pessoas
#resto

achei('#EcossistemaFIS','Mulheres na Saúde FIS', 'RJ by #FIS', 'Saúde Digital, Medicina Conectada & Novas Tecnologias')

#removendo espaços dos títulos das colunas
new_columns = {col: col.replace(' ', '') for col in getpessoas.columns}
getpessoas.rename(columns=new_columns, inplace=True)

new_columns = {col: col.replace('-', '') for col in getpessoas.columns}
getpessoas.rename(columns=new_columns, inplace=True)

getpessoas.columns = getpessoas.columns.str.lower()  



  