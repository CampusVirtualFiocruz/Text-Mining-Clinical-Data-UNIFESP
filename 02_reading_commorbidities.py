import pandas as pd
import json
import re
from unidecode import unidecode
from tqdm import tqdm

def clean_str( str ):
  try:
    str = re.sub("[\"\"]", " ", str)
    str = re.sub("[\.]", " ", str)
    str = re.sub("[^a-zA-ZãÃçÇéÉóÓ]", " ", str)
    return str.lstrip().rstrip()
  except TypeError:
    pass

with open('dicionario.json') as user_file:
  file_contents = user_file.read()
data = json.loads(file_contents)


df = pd.read_csv("lista_final_comorbidades.csv")

df_comorbidades = df["comorbidades_usomedicacao"].to_frame().fillna("")


def check_terms(string, terms, list):
    comorbidades_encontradas = []
    for term in terms:
        pattern = r'\b{}\b'.format(re.escape(term), re.IGNORECASE)
        if re.search(pattern, string, re.IGNORECASE):
            comorbidades_encontradas.append(comorbidade["sinonimo"])
    if len(comorbidades_encontradas) > 0 :
      list.append(comorbidades_encontradas)

def flatten_list(list_of_lists):
    return [item for sublist in list_of_lists for item in sublist]

df_comorbidades["comorbidades_usomedicacao"] = df_comorbidades["comorbidades_usomedicacao"].apply(unidecode)

for index in tqdm(range(len(df_comorbidades["comorbidades_usomedicacao"]))):
   comorbidades = []
   texto = df_comorbidades.at[index,"comorbidades_usomedicacao"]
   texto_cln = clean_str(texto)
   for comorbidade in data["comorbidade"]:
      check_terms(texto_cln,comorbidade["termos"],comorbidades)
   comorbidades = flatten_list(comorbidades)
   if "tabagismo" in comorbidades and "ex tabagista" in comorbidades:
      comorbidades.remove("tabagismo")
   for item in comorbidades:
      df_comorbidades.at[index,item] = 1
   for linha in texto.splitlines():
    comorbidades_negadas = []
    x = re.search(r'nega (.*)', linha.lower(), re.IGNORECASE)
    if x != None:
        for comorbidade in data["comorbidade"]:
            for termo in comorbidade["termos"]:
                if re.search(termo, x.group(1), re.IGNORECASE):
                    comorbidades_negadas.append(comorbidade["sinonimo"].lower())
    if len(comorbidades_negadas) > 0:
       for c in comorbidades_negadas:
          if c in comorbidades:
             df_comorbidades.at[index,c] = -1
   

df_comorbidades = df_comorbidades.fillna(0)

df_comorbidades.to_excel("comorbidade_unifesp_leitura_automática.xlsx")
      



   
       



