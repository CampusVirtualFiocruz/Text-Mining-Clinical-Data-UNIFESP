#contagem de frequência de palavras agrupadas por fonemas

#instalação de bibliotecas
import pandas as pd
import re
import nltk
nltk.download("punkt")
from nltk.util import ngrams
from metaphoneptbr import phonetic
from tqdm import tqdm


#leitura banco vigvac
df = pd.read_parquet("/home/zatti/Desktop/campo_aberto_vigvac/db_sintomas_morbidades.parquet")

#filtro selecionando somente as colunas de campo aberto
df_comorbidade = df.head["morb_desc"].to_frame()

#deletar os campos com None para comorbidades
index_deletar = []
lista_comorbidade = df_comorbidade["morb_desc"].to_list()
for i in range(len(lista_comorbidade)):
  celula = lista_comorbidade[i]
  if celula == None:
    index_deletar.append(i)
df_comorbidade = df_comorbidade.drop(index=index_deletar)


#limpeza de caracteres especiais
def clean_str( str ):
  try:
    str = re.sub("[\"\"]", " ", str)
    str = re.sub("[\.]", " ", str)
    str = re.sub("[^a-zA-Z]", " ", str)
    return str.lstrip().rstrip()
  except TypeError:
    pass
df_comorbidade ["morb_desc"] = df_comorbidade ["morb_desc"].apply(clean_str)
lista_comorbidade = df_comorbidade["morb_desc"].to_list()


#função para extrair n-gramas
def extract_ngrams(data, num):
    n_grams = ngrams(nltk.word_tokenize(data), num)
    return [ " ".join(grams) for grams in n_grams]

# criando colunas para n-gramas de 1 a 4.
df_comorbidade["1-grama"] = ""
df_comorbidade["2-grama"] = ""
df_comorbidade["3-grama"] = ""
df_comorbidade["4-grama"] = ""


#comorbidades
um_grama = []
dois_grama = []
tres_grama = []
quatro_grama = []

print("extração de n-gramas")
for i in tqdm(range(len(lista_comorbidade))):
  comorbidade = str(lista_comorbidade[i])
  um_grama.append(extract_ngrams(comorbidade,1))
  dois_grama.append(extract_ngrams(comorbidade,2))
  tres_grama.append(extract_ngrams(comorbidade,3))
  quatro_grama.append(extract_ngrams(comorbidade,4))

df_comorbidade["1-grama"] = um_grama
df_comorbidade["2-grama"] = dois_grama
df_comorbidade["3-grama"] = tres_grama
df_comorbidade["4-grama"] = quatro_grama

#explode e criação de dataframes separados
df_1grama_comorbidade = df_comorbidade["1-grama"].explode().to_frame().dropna()
df_1grama_comorbidade["fonema"] = ""
df_1grama_comorbidade_lista = df_1grama_comorbidade["1-grama"].to_list()

df_2grama_comorbidade = df_comorbidade["2-grama"].explode().to_frame().dropna()
df_2grama_comorbidade["fonema"] = ""
df_2grama_comorbidade_lista = df_2grama_comorbidade["2-grama"].to_list()


df_3grama_comorbidade = df_comorbidade["3-grama"].explode().to_frame().dropna()
df_3grama_comorbidade["fonema"] = ""
df_3grama_comorbidade_lista = df_3grama_comorbidade["3-grama"].to_list()


df_4grama_comorbidade = df_comorbidade["4-grama"].explode().to_frame().dropna()
df_4grama_comorbidade["fonema"] = ""
df_4grama_comorbidade_lista = df_4grama_comorbidade["4-grama"].to_list()


print("fonema 1-grama")
for i in tqdm(range(len(df_1grama_comorbidade_lista))):
  celula = df_1grama_comorbidade_lista[i]
  lista_1grama = (str(celula).split(" "))
  palavra_fonema = []
  for x in lista_1grama:
    palavra_fonema.append(phonetic(x))
  df_1grama_comorbidade.iloc[i,1] = " ".join(palavra_fonema)

print("fonema 2-grama")
for i in tqdm(range(len(df_2grama_comorbidade_lista))):
  celula = df_2grama_comorbidade_lista[i]
  lista_digrama = (str(celula).split(" "))
  palavra_fonema = []
  for x in lista_digrama:
    palavra_fonema.append(phonetic(x))
  df_2grama_comorbidade.iloc[i,1] = " ".join(palavra_fonema)

print("fonema 3-grama")
for i in tqdm(range(len(df_3grama_comorbidade_lista))):
  celula = df_3grama_comorbidade_lista[i]
  lista_trigrama = (str(celula).split(" "))
  palavra_fonema = []
  for x in lista_trigrama:
    palavra_fonema.append(phonetic(x))
  df_3grama_comorbidade.iloc[i,1] = " ".join(palavra_fonema)

print("fonema 4-grama")
for i in tqdm(range(len(df_4grama_comorbidade_lista))):
  celula = df_4grama_comorbidade_lista[i]
  lista_quadrigrama = (str(celula).split(" "))
  palavra_fonema = []
  for x in lista_quadrigrama:
    palavra_fonema.append(phonetic(x))
  df_4grama_comorbidade.iloc[i,1] = " ".join(palavra_fonema)


#contagem de frqueência de n gramas de 1 a 4 em comorbidades
df1_comorbidade = df_1grama_comorbidade.value_counts().to_frame().reset_index().rename(columns = {0:"count"})
df2_comorbidade = df_2grama_comorbidade.value_counts().to_frame().reset_index().rename(columns = {0:"count"})
df3_comorbidade = df_3grama_comorbidade.value_counts().to_frame().reset_index().rename(columns = {0:"count"})
df4_comorbidade = df_4grama_comorbidade.value_counts().to_frame().reset_index().rename(columns = {0:"count"})

#agrupamento dos termos enquadrados em cada fonema específico
#1-grama comorbidade
df1_comorbidade_fonema = df1_comorbidade.groupby("fonema")["count"].aggregate("sum").to_frame().sort_values(by=["count"], ascending = False).reset_index()
df1_comorbidade_fonema["termos"] = ""
lista_fonemas = df1_comorbidade["fonema"].to_list()
lista_fonemas_f = df1_comorbidade_fonema["fonema"].to_list()
lista_termos = df1_comorbidade["1-grama"].to_list()
coluna_termos = []
print("agrupamento 1-grama")
for i in tqdm(range(len(lista_fonemas_f))):
  termos = []
  fonema_f = lista_fonemas_f[i]


  for x in range(len(lista_fonemas)):
    fonema = lista_fonemas[x]
    termo = lista_termos[x]
    if fonema == fonema_f:
      termos.append(termo)
  
  coluna_termos.append(termos)
df1_comorbidade_fonema["termos"] = coluna_termos

#2-grama comorbidade
df2_comorbidade_fonema = df2_comorbidade.groupby("fonema")["count"].aggregate("sum").to_frame().sort_values(by=["count"], ascending = False).reset_index()
df2_comorbidade_fonema["termos"] = ""
lista_fonemas = df2_comorbidade["fonema"].to_list()
lista_fonemas_f = df2_comorbidade_fonema["fonema"].to_list()
lista_termos = df2_comorbidade["2-grama"].to_list()
coluna_termos = []
print("agrupamento 2-grama")
for i in tqdm(range(len(lista_fonemas_f))):
  termos = []
  fonema_f = lista_fonemas_f[i]


  for x in range(len(lista_fonemas)):
    fonema = lista_fonemas[x]
    termo = lista_termos[x]
    if fonema == fonema_f:
      termos.append(termo)
  
  coluna_termos.append(termos)
df2_comorbidade_fonema["termos"] = coluna_termos

#3-grama comorbidade
df3_comorbidade_fonema = df3_comorbidade.groupby("fonema")["count"].aggregate("sum").to_frame().sort_values(by=["count"], ascending = False).reset_index()
df3_comorbidade_fonema["termos"] = ""

lista_fonemas = df3_comorbidade["fonema"].to_list()
lista_fonemas_f = df3_comorbidade_fonema["fonema"].to_list()
lista_termos = df3_comorbidade["3-grama"].to_list()
coluna_termos = []
print("agrupamento 3-grama")
for i in tqdm(range(len(lista_fonemas_f))):
  termos = []
  fonema_f = lista_fonemas_f[i]


  for x in range(len(lista_fonemas)):
    fonema = lista_fonemas[x]
    termo = lista_termos[x]
    if fonema == fonema_f:
      termos.append(termo)
  
  coluna_termos.append(termos)
df3_comorbidade_fonema["termos"] = coluna_termos

#4-grama comorbidade
df4_comorbidade_fonema = df4_comorbidade.groupby("fonema")["count"].aggregate("sum").to_frame().sort_values(by=["count"], ascending = False).reset_index()
df4_comorbidade_fonema["termos"] = ""
lista_fonemas = df4_comorbidade["fonema"].to_list()
lista_fonemas_f = df4_comorbidade_fonema["fonema"].to_list()
lista_termos = df4_comorbidade["4-grama"].to_list()
coluna_termos = []
print("agrupamento 4-grama")
for i in tqdm(range(len(lista_fonemas_f))):
  termos = []
  fonema_f = lista_fonemas_f[i]


  for x in range(len(lista_fonemas)):
    fonema = lista_fonemas[x]
    termo = lista_termos[x]
    if fonema == fonema_f:
      termos.append(termo)
  
  coluna_termos.append(termos)
df4_comorbidade_fonema["termos"] = coluna_termos




#criação de parquet 
df1_comorbidade_fonema.to_parquet("/home/zatti/Desktop/campo_aberto_vigvac/count_fonema_comorbidades/df1.parquet")
df2_comorbidade_fonema.to_parquet("/home/zatti/Desktop/campo_aberto_vigvac/count_fonema_comorbidades/df2.parquet")
df3_comorbidade_fonema.to_parquet("/home/zatti/Desktop/campo_aberto_vigvac/count_fonema_comorbidades/df3.parquet")
df4_comorbidade_fonema.to_parquet("/home/zatti/Desktop/campo_aberto_vigvac/count_fonema_comorbidades/df4.parquet")

