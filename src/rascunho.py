#!/usr/bin/env python3

"""
Python script to show where the economists are located (working) in Brazil
"""


import pyspark
import pandas as pd
import math

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, udf
from pyspark.sql.types import FloatType, StringType, IntegerType

import geopandas as gpd
import matplotlib.pyplot as plt

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

# rais_br.to_csv(r'data/rais_br_econ.csv')
# rais_br = pd.read_csv('data/rais_br_econ.csv')

rais_br.shape
rais_br = rais_br.rename({"mun_trab": "cd_mun_rais"}, axis=1) # This must be in rais_etl
rais_br.cd_mun_rais = rais_br.cd_mun_rais.astype(str)

# Localidades IBGE
local = get_loc(["all"])
local.columns
local["cd_mun_rais"] = local["id"].astype(str).str[:-1]

# Join 1
df = rais_br.merge(local, on="cd_mun_rais", how="inner")

# Group_by rg_imediata
dfs = pd.DataFrame(
    {
        "cd_rg_imediata": df.groupby("regiao_imediata_id")["vl_remun_media_nom"].mean().index.astype(str), 
        "nm_rg_imediata": df.groupby("regiao_imediata_nome")["vl_remun_media_nom"].mean().index,
        "mean_econ_remu": df.groupby("regiao_imediata_id")["vl_remun_media_nom"].mean().astype(int), 
        "qt_econ": df.groupby("regiao_imediata_id")["vl_remun_media_nom"].count(),
        "log_qt_econ": df.groupby("regiao_imediata_id")["vl_remun_media_nom"].count().apply(lambda x: math.log(x))
    }
).reset_index(drop=True)

# Shapefile BR_RG_Imediatas_2020
# sc = gpd.read_file("data/SC_Municipios_2020/SC_Municipios_2020.shp")
br = gpd.read_file("data/BR_RG_Imediatas_2020/BR_RG_Imediatas_2020.shp")

br['cd_rg_imediata'] = br['CD_RGI'].astype(str)
br.cd_rg_imediata = br.cd_rg_imediata.astype(str)

# Join with .shp
dfshp = br.merge(dfs, on='cd_rg_imediata', how="inner")

dfshp.plot(column='qt_econ', cmap='OrRd', edgecolor='k', legend=True)
dfshp.plot(column='mean_econ_remu', cmap='OrRd', edgecolor='k', legend=True)
dfshp.plot(column='log_qt_econ', cmap='OrRd', edgecolor='k', legend=True)
plt.show()


