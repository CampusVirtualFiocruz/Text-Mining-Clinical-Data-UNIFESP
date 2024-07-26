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
df = pd.read_parquet("/home/zatti/Área de Trabalho/vigvac_campo_aberto/db_sintomas_morbidades.parquet")

#filtro selecionando somente as colunas de campo aberto
df_sintoma = df["outro_des"].to_frame()

#deletar None para sintomas
index_deletar = []
lista_sintoma = df_sintoma["outro_des"].to_list()
for i in range(len(lista_sintoma)):
  celula = lista_sintoma[i]
  if celula == None:
    index_deletar.append(i)
df_sintoma = df_sintoma.drop(index=index_deletar)


#limpeza de caracteres especiais
def clean_str( str ):
  try:
    str = re.sub("[\"\"]", ",", str)
    str = re.sub("[\.]", ",", str)
    str = re.sub("[^a-zA-Z]", ",", str)
    return str.lstrip().rstrip()
  except TypeError:
    pass
df_sintoma ["outro_des"] = df_sintoma["outro_des"].apply(clean_str)
lista_sintoma = df_sintoma["outro_des"].to_list()


#função para extrair n-gramas
def extract_ngrams(data, num):
    n_grams = ngrams(nltk.word_tokenize(data), num)
    return [ ",".join(grams) for grams in n_grams]

#criando colunas para n-gramas de 1 a 4
df_sintoma["1-grama"] = ""
df_sintoma["2-grama"] = ""
df_sintoma["3-grama"] = ""
df_sintoma["4-grama"] = ""


#sintomas
um_grama = []
dois_grama = []
tres_grama = []
quatro_grama = []

print("extração n-gramas")
for i in tqdm(range(len(lista_sintoma))):
  sintoma = str(lista_sintoma[i])
  um_grama.append(extract_ngrams(sintoma,1))
  dois_grama.append(extract_ngrams(sintoma,2))
  tres_grama.append(extract_ngrams(sintoma,3))
  quatro_grama.append(extract_ngrams(sintoma,4))

df_sintoma["1-grama"] = um_grama
df_sintoma["2-grama"] = dois_grama
df_sintoma["3-grama"] = tres_grama
df_sintoma["4-grama"] = quatro_grama

#explode e criação de dataframes separados
df_1grama_sintoma = df_sintoma["1-grama"].explode().to_frame().dropna()
df_1grama_sintoma["fonema"] = ""
df_1grama_sintoma_lista = df_1grama_sintoma["1-grama"].to_list()

df_2grama_sintoma = df_sintoma["2-grama"].explode().to_frame().dropna()
df_2grama_sintoma["fonema"] = ""
df_2grama_sintoma_lista = df_2grama_sintoma["2-grama"].to_list()


df_3grama_sintoma = df_sintoma["3-grama"].explode().to_frame().dropna()
df_3grama_sintoma["fonema"] = ""
df_3grama_sintoma_lista = df_3grama_sintoma["3-grama"].to_list()


df_4grama_sintoma = df_sintoma["4-grama"].explode().to_frame().dropna()
df_4grama_sintoma["fonema"] = ""
df_4grama_sintoma_lista = df_4grama_sintoma["4-grama"].to_list()


print("fonema 1-grama")
for i in tqdm(range(len(df_1grama_sintoma_lista))):
  celula = df_1grama_sintoma_lista[i]
  lista_1grama = (str(celula).split(","))
  palavra_fonema = []
  for x in lista_1grama:
    palavra_fonema.append(phonetic(x))
  df_1grama_sintoma.iloc[i,1] = ",".join(palavra_fonema)

print("fonema 2-grama")
for i in tqdm(range(len(df_2grama_sintoma_lista))):
  celula = df_2grama_sintoma_lista[i]
  lista_digrama = (str(celula).split(","))
  palavra_fonema = []
  for x in lista_digrama:
    palavra_fonema.append(phonetic(x))
  df_2grama_sintoma.iloc[i,1] = ",".join(palavra_fonema)

print("fonema 3-grama")
for i in tqdm(range(len(df_3grama_sintoma_lista))):
  celula = df_3grama_sintoma_lista[i]
  lista_trigrama = (str(celula).split(","))
  palavra_fonema = []
  for x in lista_trigrama:
    palavra_fonema.append(phonetic(x))
  df_3grama_sintoma.iloc[i,1] = ",".join(palavra_fonema)

print("fonema 4-grama")
for i in tqdm(range(len(df_4grama_sintoma_lista))):
  celula = df_4grama_sintoma_lista[i]
  lista_quadrigrama = (str(celula).split(","))
  palavra_fonema = []
  for x in lista_quadrigrama:
    palavra_fonema.append(phonetic(x))
  df_4grama_sintoma.iloc[i,1] = ",".join(palavra_fonema)


#contagem de frqueência de n gramas de 1 a 4 em sintomas
df1_sintoma = df_1grama_sintoma.value_counts().to_frame().reset_index().rename(columns = {0:"count"})
df2_sintoma = df_2grama_sintoma.value_counts().to_frame().reset_index().rename(columns = {0:"count"})
df3_sintoma = df_3grama_sintoma.value_counts().to_frame().reset_index().rename(columns = {0:"count"})
df4_sintoma = df_4grama_sintoma.value_counts().to_frame().reset_index().rename(columns = {0:"count"})

#agrupamento dos termos enquadrados em cada fonema específico
#1-grama sintoma
df1_sintoma_fonema = df1_sintoma.groupby("fonema")["count"].aggregate("sum").to_frame().sort_values(by=["count"], ascending = False).reset_index()
df1_sintoma_fonema["termos"] = ""
lista_fonemas = df1_sintoma["fonema"].to_list()
lista_fonemas_f = df1_sintoma_fonema["fonema"].to_list()
lista_termos = df1_sintoma["1-grama"].to_list()
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
df1_sintoma_fonema["termos"] = coluna_termos

#2-grama comorbidade
df2_sintoma_fonema = df2_sintoma.groupby("fonema")["count"].aggregate("sum").to_frame().sort_values(by=["count"], ascending = False).reset_index()
df2_sintoma_fonema["termos"] = ""
lista_fonemas = df2_sintoma["fonema"].to_list()
lista_fonemas_f = df2_sintoma_fonema["fonema"].to_list()
lista_termos = df2_sintoma["2-grama"].to_list()
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
df2_sintoma_fonema["termos"] = coluna_termos

#3-grama comorbidade
df3_sintoma_fonema = df3_sintoma.groupby("fonema")["count"].aggregate("sum").to_frame().sort_values(by=["count"], ascending = False).reset_index()
df3_sintoma_fonema["termos"] = ""

lista_fonemas = df3_sintoma["fonema"].to_list()
lista_fonemas_f = df3_sintoma_fonema["fonema"].to_list()
lista_termos = df3_sintoma["3-grama"].to_list()
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
df3_sintoma_fonema["termos"] = coluna_termos

#4-grama comorbidade
df4_sintoma_fonema = df4_sintoma.groupby("fonema")["count"].aggregate("sum").to_frame().sort_values(by=["count"], ascending = False).reset_index()
df4_sintoma_fonema["termos"] = ""
lista_fonemas = df4_sintoma["fonema"].to_list()
lista_fonemas_f = df4_sintoma_fonema["fonema"].to_list()
lista_termos = df4_sintoma["4-grama"].to_list()
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
df4_sintoma_fonema["termos"] = coluna_termos



#criação de parquet 

df1_sintoma_fonema.to_parquet("/home/zatti/Área de Trabalho/vigvac_campo_aberto/count_fonema_sintomas/df1.parquet")
df2_sintoma_fonema.to_parquet("/home/zatti/Área de Trabalho/vigvac_campo_aberto/count_fonema_sintomas/df2.parquet")
df3_sintoma_fonema.to_parquet("/home/zatti/Área de Trabalho/vigvac_campo_aberto/count_fonema_sintomas/df3.parquet")
df4_sintoma_fonema.to_parquet("/home/zatti/Área de Trabalho/vigvac_campo_aberto/count_fonema_sintomas/df4.parquet")
