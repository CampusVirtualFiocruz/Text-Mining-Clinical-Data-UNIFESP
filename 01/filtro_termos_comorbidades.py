import pandas as pd

df1 = pd.read_parquet("/home/zatti/Área de Trabalho/campo_aberto_vigvac/count_fonema_comorbidades/df1.parquet")
df2 = pd.read_parquet("/home/zatti/Área de Trabalho/campo_aberto_vigvac/count_fonema_comorbidades/df2.parquet")
df3 = pd.read_parquet("/home/zatti/Área de Trabalho/campo_aberto_vigvac/count_fonema_comorbidades/df3.parquet")
df4 = pd.read_parquet("/home/zatti/Área de Trabalho/campo_aberto_vigvac/count_fonema_comorbidades/df4.parquet")


df1.to_excel("/home/zatti/Área de Trabalho/campo_aberto_vigvac/count_fonema_comorbidades/df filtrados/df1.xlsx")
df2.to_excel("/home/zatti/Área de Trabalho/campo_aberto_vigvac/count_fonema_comorbidades/df filtrados/df2.xlsx")
df3.to_excel("/home/zatti/Área de Trabalho/campo_aberto_vigvac/count_fonema_comorbidades/df filtrados/df3.xlsx")
df4.to_excel("/home/zatti/Área de Trabalho/campo_aberto_vigvac/count_fonema_comorbidades/df filtrados/df4.xlsx")
