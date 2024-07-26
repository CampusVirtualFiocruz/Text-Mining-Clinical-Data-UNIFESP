from Levenshtein import distance
from metaphoneptbr import phonetic
import json
import pandas as pd
import re
from tqdm import tqdm

with open("dicionario.json") as user_file:
  file_contents = user_file.read()
data = json.loads(file_contents)

df = pd.read_excel("sintomas_normalizado.xlsx")

df_hpma = df.fillna("")

def check_terms(string, terms, list):
    sintomas_encontrados = []
    for term in terms:
        pattern = r"\b{}\b".format(re.escape(term), re.IGNORECASE)
        if re.search(pattern, string, re.IGNORECASE):
            sintomas_encontrados.append(sintoma["sinonimo"])
    if len(sintomas_encontrados) > 0 :
      list.append(sintomas_encontrados)

def flatten_list(list_of_lists):
    return [item for sublist in list_of_lists for item in sublist]



for index in tqdm(range(len(df_hpma["hpma_normalizada"]))):
  
  sintomas = []
  celula = df_hpma.at[index,"hpma_normalizada"]
  for sintoma in data["sintoma"]:
     check_terms(celula, sintoma["termos"],sintomas)
  pattern = r"(?<=nega)(.*?)(?=\.|nega)"
  x = re.findall(pattern, celula, re.IGNORECASE)
  sintomas = flatten_list(sintomas)

  sintomas_negados = []
  for i in x:
    for sintoma in data["sintoma"]:
        check_terms(i,sintoma["termos"],sintomas_negados)
  sintomas_negados = flatten_list(sintomas_negados)

  if len(sintoma) > 0:
     for i in sintomas:
        df_hpma.at[index,i] = 1
  if len(sintomas_negados) > 0:
     for i in sintomas_negados:
        df_hpma.at[index,i] = -1

df_hpma.to_excel("leitura_sintomas_auto_normalizada.xlsx")



  

  
  

