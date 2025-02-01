# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 15:56:03 2025

@author: lauta
"""

import csv
def leer_parque(nombre_archivo, parque):
    lista_de_dicio = []
    f = open(nombre_archivo)
    filas = csv.reader(f)
    for fila in filas:
        dicio = {}
        if fila[10] == parque:
            dicio[fila[2]] = fila
            lista_de_dicio += [dicio]
    f.close()
    return lista_de_dicio
res = leer_parque(nombre_archivo, "GENERAL PAZ")
print(len(res))
print(res)



import pandas as pd

nombre_archivo = 'arbolado-en-espacios-verdes.csv'
df = pd.read_csv(nombre_archivo)
p = df['nombre_com'].tolist()

def especies(lista_arboles):
    especies = []
    for a in p:
        if a not in especies:
            especies += [a]
    return especies

print(len(especies(p)))











