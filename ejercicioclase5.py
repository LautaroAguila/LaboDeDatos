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


#h
casos_por_depto =dd.sql( """
                SELECT DISTINCT c1.id_depto, d.id_provincia, c1.cantidad, c1.anio
                FROM casos AS c1
                INNER JOIN departamento AS d
                ON d.id=c1.id_depto
              """).df()
inerdepto = dd.sql("""
                SELECT DISTINCT c.*, p.descripcion
                FROM casos_por_depto AS c
                INNER JOIN provincia AS p
                ON c.id_provincia=p.id
              """).df()
consultaSQL = """
                SELECT DISTINCT i.id_provincia,i.anio, MAX(cantidad) AS max
                FROM inerdepto AS i
                GROUP BY id_provincia, anio
                ORDER BY id_provincia
              """

dataframeResultado = dd.sql(consultaSQL).df()


#i
consultaSQL = """
                SELECT DISTINCT anio, SUM(cantidad) AS total, MAX(cantidad) AS maximo, MIN(cantidad) AS minimo, AVG(cantidad) AS promedio 
                FROM casos
                INNER JOIN departamento AS d
                ON id_depto=d.id
                WHERE id_provincia IN (
                    SELECT DISTINCT p.id
                    FROM provincia AS p
                    WHERE p.descripcion='Buenos Aires'
                    ) AND anio='2019'
                GROUP BY anio
              """

dataframeResultado = dd.sql(consultaSQL).df()


#j
punto_i = dd.sql("""
                SELECT DISTINCT anio, SUM(cantidad) AS total, MAX(cantidad) AS maximo, MIN(cantidad) AS minimo, AVG(cantidad) AS promedio 
                FROM casos
                INNER JOIN departamento AS d
                ON id_depto=d.id
                WHERE id_provincia IN (
                    SELECT DISTINCT p.id
                    FROM provincia AS p
                    WHERE p.descripcion='Buenos Aires'
                    ) AND anio='2019'
                GROUP BY anio
              """).df()
consultaSQL = """
                SELECT DISTINCT *
                FROM punto_i
                WHERE total>1000
              """

dataframeResultado = dd.sql(consultaSQL).df()


#k
consultaSQL = """
                     SELECT DISTINCT cd.descripcion AS nombreDepto, p.descripcion AS nombreProv, AVG(cd.cantidad) AS promedio
                     FROM provincia AS  p
                     INNER JOIN (
                         SELECT d.descripcion, d.id_provincia, c.cantidad
                         FROM casos AS c
                         INNER JOIN departamento AS d
                         ON d.id=c.id_depto
                         WHERE c.anio=2020 OR c.anio=2019
                         GROUP BY d.descripcion, d.id_provincia, c.cantidad
                         ) AS cd
                    ON cd.id_provincia=p.id
                    GROUP BY cd.descripcion, p.descripcion
                    ORDER BY p.descripcion ASC, cd.descripcion ASC
              """

dataframeResultado = dd.sql(consultaSQL).df()


#l 
consultaSQL = """
                SELECT DISTINCT id_depto, des_depto, id_prov, des_prov, t.descripcion AS des_ev, SUM(CASE WHEN dpc.anio='2020' THEN cantidad ELSE 0 END ) AS tot_2020, SUM(CASE WHEN dpc.anio='2019' THEN cantidad ELSE 0 END ) AS tot_2019
                FROM tipoevento as t
                INNER JOIN (
                        SELECT dp.*, c.anio, c.cantidad , c.id_tipoevento AS id_ev
                        FROM casos as c
                        INNER JOIN(
                            SELECT d.id AS id_depto, p.id AS id_prov, d.descripcion AS des_depto, p.descripcion AS des_prov
                            FROM departamento AS d
                            INNER JOIN provincia AS p
                            ON d.id_provincia=p.id
                            ) AS dp
                        ON c.id_depto=dp.id_depto
                    ) AS dpc
                ON t.id=dpc.id_ev
                GROUP BY id_depto, des_depto, id_prov, des_prov, t.descripcion
                ORDER BY tot_2020 DESC
              """

dataframeResultado = dd.sql(consultaSQL).df()


###
#5a
consultaSQL = """
                SELECT DISTINCT c.cantidad, d.id AS id_depto
                FROM casos AS c
                INNER JOIN departamento AS d
                ON c.id_depto=d.id
                WHERE c.cantidad >= ALL(
                    SELECT DISTINCT cantidad
                    FROM casos
                    )
              """

dataframeResultado = dd.sql(consultaSQL).df()


#b
consultaSQL = """
                SELECT DISTINCT t.descripcion
                FROM tipoevento AS t
                WHERE t.id = ANY (
                    SELECT DISTINCT t.id
                    FROM casos AS c
                    INNER JOIN tipoevento AS t
                    ON c.id_tipoevento=t.id
                    )
               
              """

dataframeResultado = dd.sql(consultaSQL).df()


###
#6a
consultaSQL = """
                SELECT DISTINCT t.descripcion
                FROM tipoevento AS t
                WHERE t.id IN (
                    SELECT DISTINCT t.id
                    FROM casos AS c
                    INNER JOIN tipoevento AS t
                    ON c.id_tipoevento=t.id
                    )
              """

dataframeResultado = dd.sql(consultaSQL).df()


#b
consultaSQL = """
                SELECT DISTINCT t.descripcion
                FROM tipoevento AS t
                WHERE t.id NOT IN (
                    SELECT DISTINCT t.id
                    FROM casos AS c
                    INNER JOIN tipoevento AS t
                    ON c.id_tipoevento=t.id
                    )
              """

dataframeResultado = dd.sql(consultaSQL).df()


###
#7a
consultaSQL = """
                SELECT DISTINCT t.descripcion
                FROM tipoevento AS t
                WHERE EXISTS (
                    SELECT 1 FROM casos AS c WHERE c.id_tipoevento=t.id
                    )
              """

dataframeResultado = dd.sql(consultaSQL).df()


#b
consultaSQL = """
                SELECT DISTINCT t.descripcion
                FROM tipoevento AS t
                WHERE NOT EXISTS (
                    SELECT 1 FROM casos AS c WHERE c.id_tipoevento=t.id
                    )
              """

dataframeResultado = dd.sql(consultaSQL).df()


###
#8a
a = dd.sql("""
           SELECT DISTINCT AVG(cantidad) AS promedioPais
           FROM casos
           
           """).df()
promedio_pais = a['promedioPais'].iloc[0]
consultaSQL = """
                SELECT DISTINCT p.descripcion, c.cantidad, c.anio
                FROM casos AS c
                INNER JOIN (
                    SELECT p.descripcion, d.id AS id_dep
                    FROM provincia AS p
                    INNER JOIN departamento AS d
                    ON p.id=d.id_provincia
                    ) AS p
                ON p.id_dep=c.id_depto
                WHERE c.cantidad>""" +str(promedio_pais) + """
                GROUP BY p.descripcion, c.cantidad, c.anio
                """

dataframeResultado = dd.sql(consultaSQL).df()


#b
a = dd.sql("""
           SELECT DISTINCT SUM(c.cantidad) AS tot_Corr
           FROM casos AS c
           INNER JOIN (
               SELECT d.id AS id_dep, p.id, p.descripcion
               FROM provincia AS p
               INNER JOIN departamento AS d
               ON p.id=d.id_provincia
               WHERE p.descripcion='Corrientes'
               ) AS dp
           ON c.id_depto=dp.id_dep
           """).df()
tot_Corr = a['tot_Corr'].iloc[0]

b = dd.sql("""
           SELECT DISTINCT c.anio, p.descripcion, SUM(c.cantidad) AS cant_prov
           FROM casos AS c
           INNER JOIN (
               SELECT p.descripcion, d.id AS id_dep
               FROM departamento AS d
               INNER JOIN provincia AS p
               ON p.id=d.id_provincia
               ) AS p
           ON c.id_depto=p.id_dep
           GROUP BY c.anio, p.descripcion
           
           """).df()
           
           
consultaSQL = """
                SELECT DISTINCT *
                FROM b
                WHERE cant_prov>""" +str(tot_Corr) + """
                
                """

dataframeResultado = dd.sql(consultaSQL).df()


###
#9a
consultaSQL = """
                SELECT DISTINCT *
                FROM departamento
                ORDER BY descripcion ASC, id ASC
                """

dataframeResultado = dd.sql(consultaSQL).df()


#b
consultaSQL = """
                SELECT DISTINCT *
                FROM provincia
                WHERE descripcion LIKE 'M%'
                """

dataframeResultado = dd.sql(consultaSQL).df()


#c
consultaSQL = """
                SELECT DISTINCT *
                FROM provincia
                WHERE descripcion LIKE 'S____a%'
                """

dataframeResultado = dd.sql(consultaSQL).df()


#d
consultaSQL = """
                SELECT DISTINCT *
                FROM provincia
                WHERE descripcion LIKE '%a'
                """

dataframeResultado = dd.sql(consultaSQL).df()


#e
consultaSQL = """
                SELECT DISTINCT *
                FROM provincia
                WHERE descripcion LIKE '_____'
                """

dataframeResultado = dd.sql(consultaSQL).df()


#f
consultaSQL = """
                SELECT DISTINCT *
                FROM provincia
                WHERE descripcion LIKE '%do%'
                """

dataframeResultado = dd.sql(consultaSQL).df()


#g
consultaSQL = """
                SELECT DISTINCT *
                FROM provincia
                WHERE descripcion LIKE '%do%' AND id<30
                """

dataframeResultado = dd.sql(consultaSQL).df()


#h
consultaSQL = """
                SELECT DISTINCT id AS codigo_depto, descripcion AS nombre_depto
                FROM departamento
                WHERE descripcion LIKE '%San%'
                ORDER BY descripcion DESC
                """

dataframeResultado = dd.sql(consultaSQL).df()


#i

dp = dd.sql("""
                SELECT p.descripcion AS nom_prov, d.descripcion AS nom_dep, d.id AS id
                    FROM departamento AS d
                    INNER JOIN provincia AS p
                    ON d.id_provincia=p.id

            """).df()
ec = dd.sql("""
                    SELECT c.id_depto AS id, c.anio, c.semana_epidemiologica AS sem_epi, c.cantidad, g.descripcion AS gru_etar
                    FROM casos AS c
                    INNER JOIN grupoetario AS g
                    ON c.id_grupoetario=g.id

            """).df()
consultaSQL = """
                SELECT DISTINCT *
                FROM dp
                INNER JOIN ec
                ON dp.id=ec.id
                WHERE nom_prov LIKE '%a' AND cantidad>10
                """

dataframeResultado = dd.sql(consultaSQL).df()

































