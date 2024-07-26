import json
import pandas as pd
import re
import nltk
import numpy as np
nltk.download('punkt')
from nltk.util import ngrams
from tqdm import tqdm
from metaphoneptbr import phonetic


#função de extração de n-gramas
def extract_ngrams(data, num):
    n_grams = ngrams(nltk.word_tokenize(data), num)
    return [ ' '.join(grams) for grams in n_grams]

df = pd.read_parquet("/home/zatti/Área de Trabalho/campo_aberto_vigvac/db_sintomas_morbidades.parquet")
df_comorbidades = df[["id_vigvac","morb_desc"]].copy()

#limpeza de caracteres especiais
def clean_str( str ):
  try:
    str = re.sub("[\"\"]", " ", str)
    str = re.sub("[\.]", " ", str)
    str = re.sub("[^a-zA-Z]", " ", str)
    return str.lstrip().rstrip()
  except TypeError:
    pass
df_comorbidades ["morb_desc"] = df_comorbidades["morb_desc"].apply(clean_str)

with open('dicionario.json') as user_file:
  file_contents = user_file.read()
data = json.loads(file_contents)

lista_comorbidades_aberto = list(df_comorbidades["morb_desc"].to_list())
for i in tqdm(range(len(lista_comorbidades_aberto))):
  preenchimento = str(lista_comorbidades_aberto[i])
  for n in range (1,5):
    n_grama = extract_ngrams(preenchimento,n)
    for y in n_grama:
      lista_ngrama = (str(y).split(","))
      palavra_fonema = []
      for w in lista_ngrama:
        palavra_fonema.append(phonetic(w))
      fonema_composto = ",".join(palavra_fonema)
# procurar o fonema dentro do meu dicionário de fonemas
      for termo in data["comorbidade"]:
        if (fonema_composto == termo["fonema"]) == True:
          for palavra in termo["termos"]:
            if (y == palavra) == True:
              df_comorbidades.at[i,str(termo["sinonimo"])] = 1

for i in tqdm(range(len(df_comorbidades))):
  #quando houver ex tabagista = 1 e tabagismo = 1 na mesma linha o tabagismo = 1 deve ser deletado
  extb = df_comorbidades.at[i,"ex tabagista"]
  tbjs = df_comorbidades.at[i,"tabagismo"]
  if extb == tbjs:
    df_comorbidades.at[i,"tabagismo"] = np.NaN

df_comorbidades.to_parquet("campo_aberto_comorbidade.parquet")
