#!/usr/bin/env python3

"""
Python script to show where the economists are located (working) in Brazil
"""


import pyspark
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, udf
from pyspark.sql.types import FloatType, StringType, IntegerType

from src.fct.data_loc import get_loc
from src.fct.data_rais import rais_etl


# Load RAIS data
# http://pdet.mte.gov.br/microdados-rais-e-caged
rais_br = rais_etl(
    ["data/RAIS_VINC_PUB_CENTRO_OESTE.txt",
    "data/RAIS_VINC_PUB_MG_ES_RJ.txt",
    "data/RAIS_VINC_PUB_NORDESTE.txt",
    "data/RAIS_VINC_PUB_NORTE.txt",
    "data/RAIS_VINC_PUB_SP.txt",
    "data/RAIS_VINC_PUB_SUL.txt"] 
)

rais_br.shape
rais_br = rais_br.rename({"mun_trab": "cd_mun_rais"}, axis=1)
rais_br.cd_mun_rais = rais_br.cd_mun_rais.astype(str)

local = get_loc(["all"])
local.columns

local["cd_mun_rais"] = local["id"].astype(str).str[:-1]

df = rais_br.merge(local, on="cd_mun_rais", how="inner")

dfs = df.groupby("cd_mun_rais")["idade"].count()
dfs = pd.DataFrame({"cd_mun_rais": dfs.index, "qt_econ": dfs}).reset_index(drop=True)


df2 = sc.merge(dfs, on='cd_mun_rais', how="inner")

df2.plot(column='qt_econ', cmap='OrRd', edgecolor='k', legend=True)
plt.show()


#______________________________________________________________
4205407 in list(rais_br.mun_trab)
420540 in list(rais_br.mun_trab)
410690 in list(rais_br.mun_trab.iloc[:, 1])

dd = rais_br.groupby("mun_trab")["idade"].count()
rais_summed = pd.DataFrame({"cd_mun_rais": dd.index, "qt_econ":dd}).reset_index(drop=True)



import geopandas as gpd
import matplotlib.pyplot as plt

# We use a PySAL example shapefile
import libpysal as ps

sc = gpd.read_file("data/SC_Municipios_2020/SC_Municipios_2020.shp")
sc['cd_mun_rais'] = sc['CD_MUN'].str[:-1]

sc.cd_mun_rais = sc.cd_mun_rais.astype(str)
rais_summed.cd_mun_rais = rais_summed.cd_mun_rais.astype(str)

df = sc.merge(rais_summed, on='cd_mun_rais', how="inner")

df.plot(column='qt_econ', cmap='OrRd', edgecolor='k', legend=True)
plt.show()





