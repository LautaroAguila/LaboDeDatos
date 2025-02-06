# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 15:49:46 2025

@author: lauta
"""

# Importamos bibliotecas
import pandas as pd
import duckdb as dd

casos       = pd.read_csv("casos.csv")
departamento= pd.read_csv("departamento.csv")
grupoetario = pd.read_csv("grupoetario.csv")
provincia   = pd.read_csv("provincia.csv")
tipoevento  = pd.read_csv("tipoevento.csv")



#1a
consultaSQL = """
               SELECT  DISTINCT descripcion AS nombre
               FROM departamento;
              """
dataframeResultado = dd.sql(consultaSQL).df()

#b
consultaSQL = """
               SELECT descripcion AS nombre
               FROM departamento;
              """
dataframeResultado = dd.sql(consultaSQL).df()

#c
consultaSQL = """
               SELECT  DISTINCT descripcion AS nombre, id
               FROM departamento;
              """
dataframeResultado = dd.sql(consultaSQL).df()

#d
consultaSQL = """
               SELECT  DISTINCT *
               FROM departamento;
              """
dataframeResultado = dd.sql(consultaSQL).df()

#e
consultaSQL = """
               SELECT  DISTINCT descripcion AS nombre_depto, id AS codigo_depto
               FROM departamento;
              """
dataframeResultado = dd.sql(consultaSQL).df()

#f
consultaSQL = """
               SELECT  DISTINCT *
               FROM departamento
               WHERE id_provincia=54
              """
dataframeResultado = dd.sql(consultaSQL).df()

#g
consultaSQL = """
               SELECT  DISTINCT *
               FROM departamento
               WHERE id_provincia=54 AND id_provincia=78 AND id_provincia=86 
              """
dataframeResultado = dd.sql(consultaSQL).df()

#h
consultaSQL = """
               SELECT  DISTINCT *
               FROM departamento
               WHERE id_provincia>=50 AND id_provincia<=59 
              """
dataframeResultado = dd.sql(consultaSQL).df()


####
#2ab
consultaSQL = """
               SELECT DISTINCT departamento.id, provincia.descripcion
               FROM  provincia
               INNER JOIN departamento
               ON departamento.id_provincia=provincia.id 
              """
dataframeResultado = dd.sql(consultaSQL).df()

#c
consultaSQL = """
               SELECT DISTINCT departamento.id, departamento.descripcion
               FROM  provincia
               INNER JOIN departamento
               ON departamento.id_provincia=provincia.id
               WHERE provincia.descripcion='Chaco'
              """
dataframeResultado = dd.sql(consultaSQL).df()

#d
consultaSQL = """
               SELECT DISTINCT dep_casos.descripcion, dep_casos.cantidad
               FROM  provincia
               INNER JOIN (
                   SELECT DISTINCT *
                   FROM  casos
                   INNER JOIN departamento
                   ON departamento.id=casos.id_depto
                   WHERE casos.cantidad>10
                   ) AS dep_casos
               ON id_provincia=provincia.id AND provincia.descripcion='Buenos Aires'
              """
dataframeResultado = dd.sql(consultaSQL).df()


###
#3a
consultaSQL = """
               SELECT DISTINCT d.descripcion 
                FROM departamento AS d
                LEFT JOIN casos AS c 
                ON d.id = c.id_depto
                WHERE c.id IS NULL;

              """
dataframeResultado = dd.sql(consultaSQL).df()

#b
consultaSQL = """
                SELECT DISTINCT te.descripcion 
                FROM tipoevento AS te
                LEFT JOIN casos AS c 
                ON te.id = c.id_tipoevento
                WHERE c.id IS NULL;

              """
dataframeResultado = dd.sql(consultaSQL).df()


###
#4a
consultaSQL = """
                SELECT DISTINCT SUM(casos.cantidad) AS cantidad_total
                FROM casos
                

              """
dataframeResultado = dd.sql(consultaSQL).df()


#b
consultaSQL = """
                SELECT DISTINCT tipoevento.descripcion,SUM(casos.cantidad) AS cantidad_total
                FROM casos
                
                INNER JOIN tipoevento
                ON casos.id_tipoevento=tipoevento.id
                GROUP BY descripcion, anio
              """
dataframeResultado = dd.sql(consultaSQL).df()


#c
consultaSQL = """
                SELECT DISTINCT tipoevento.descripcion,SUM(casos.cantidad) AS cantidad_total
                FROM casos
                
                INNER JOIN tipoevento
                ON casos.id_tipoevento=tipoevento.id
                WHERE anio='2019'
                GROUP BY descripcion
                
              """
dataframeResultado = dd.sql(consultaSQL).df()


#d
consultaSQL = """
                SELECT DISTINCT id_provincia, COUNT(d.id_provincia) AS cantidad_depto
                FROM departamento AS d
                GROUP BY d.id_provincia
                ORDER BY d.id_provincia ASC
              """
dataframeResultado = dd.sql(consultaSQL).df()


#e
consultaSQL = """
                SELECT d.descripcion, SUM(c.cantidad) AS total_casos
                FROM departamento d
                LEFT JOIN casos c ON d.id = c.id_depto
                WHERE c.anio = 2019
                GROUP BY d.id, d.descripcion
                ORDER BY total_casos ASC

              """
dataframeResultado = dd.sql(consultaSQL).df()


#f
consultaSQL = """
                SELECT d.descripcion, SUM(c.cantidad) AS total_casos
                FROM departamento d
                LEFT JOIN casos c ON d.id = c.id_depto
                WHERE c.anio = 2020
                GROUP BY d.id, d.descripcion
                ORDER BY total_casos DESC


              """
dataframeResultado = dd.sql(consultaSQL).df()


#g
consultaSQL = """
                SELECT p.descripcion AS provincia, c.anio, AVG(c.cantidad) AS promedio_casos
                FROM provincia p
                JOIN departamento d ON p.id = d.id_provincia
                JOIN casos c ON d.id = c.id_depto
                GROUP BY p.id, p.descripcion, c.anio
                ORDER BY c.anio, p.descripcion;
                

              """
dataframeResultado = dd.sql(consultaSQL).df()











