import json
import pandas as pd
import re
import numpy as np
from tqdm import tqdm


with open('dicionario.json') as user_file:
  file_contents = user_file.read()
data = json.loads(file_contents)


df = pd.read_parquet("srag_sintomas_timestamp.parquet")

## função para limpar as celulas de campo aberto, retirando todos os caracteres especiais
def clean_str( str ):
  try:
    str = re.sub("[\"\"]", " ", str)
    str = re.sub("[\.]", " ", str)
    str = re.sub("[^a-zA-Z]", " ", str)
    return str.lstrip().rstrip()
  except TypeError:
    pass

df ["outro_des"] = df["outro_des"].apply(clean_str)

## função que será aplicada no dataframe para criar as binárias específicas quando o termo for encontrado na celula
def criar_binarias(str):
    if str != None: 
      x = re.search(r"" + "|".join(sintoma["termos"]) , str, re.IGNORECASE)
      if x != None:
        return 1


## aplicação da função para criação da binária na coluna de nome do termo
for sintoma in tqdm(data["sintoma"]):
  df[sintoma["nome"]] = df["outro_des"].apply(criar_binarias)



## esse bloco foi necessário para agrupar as colunas dentro de seus sinonimos, p ex: mialgia e dor no corpo
df['mialgia'] = df[['mialgia','dor no corpo']].max(axis=1)
df = df.drop(["dor no corpo"], axis =1)

df["cefaleia"] = df[['cefaleia','dor de cabeca', 'dor na cabeca']].max(axis=1)
df = df.drop(['dor de cabeca', 'dor na cabeca'], axis = 1)

df["coriza"] = df["coriza"]

df['astenia'] = df[['astenia','fraqueza', "prostracao"]].max(axis=1)
df = df.drop(['fraqueza', "prostracao"], axis = 1)

df["nauseas"] = df[['nauseas','nausea']].max(axis=1)
df = df.drop(['nausea'], axis = 1)

df['inapetencia'] = df[['inapetencia','perda do apetite', 'falta de apetite', 'hiporexia']].max(axis=1)
df = df.drop(['perda do apetite', 'falta de apetite', 'hiporexia'], axis = 1)

df['alteracoes neurologicas'] = df[['desorientacao', 'confusao mental', 'confusao', 'amnesia', 'perda de memoria', 'confusao pos ictal', 'deficit de memoria']].max(axis=1)
df = df.drop(['desorientacao', 'confusao mental', 'confusao', 'amnesia', 'perda de memoria', 'confusao pos ictal', 'deficit de memoria'], axis = 1)

df["anosmia"] = df[['anosmia', 'perda de olfato']].max(axis=1)
df = df.drop(['perda de olfato'], axis=1)

df['ageusia'] = df[['ageusia','perda do paladar','falta de paladar']].max(axis=1)
df = df.drop(['perda do paladar','falta de paladar'], axis=1)

df.to_parquet("campo_aberto_sintomas.parquet")