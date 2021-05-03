#! python3

'''
Python script to show where the economists are located (working) in Brazil
'''

# libs
import pyspark
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, udf
from pyspark.sql.types import FloatType, StringType, IntegerType

# Start Spark session
spark = SparkSession.builder.appName('where_econ').getOrCreate()

def rais_etl(file: str):
    '''
    RAIS ETL
    '''

    # Load RAIS
    rais = spark.read.load(file, format="csv", sep=";", inferSchema="true", header="true")
 
    # Change column names
    newColumns = ["bairros_sp", "bairros_fortaleza", "bairros_rj", "causa_afastamento_1", "causa_afastamento_2", "causa_afastamento_3", "motivo_desligamento", "cbo_ocupacao_2002", "cnae_2_0_classe", "cnae_95_classe", "distritos_sp", "vinculo_ativo_31_12", "faixa_etaria", "faixa_hora_contrat", "faixa_remun_dezem_sm", "faixa_remun_media_sm", "faixa_tempo_emprego", "escolaridade_apos_2005", "qtd_hora_contr", "idade", "ind_cei_vinculado", "ind_simples", "mes_admissao", "mes_desligamento", "mun_trab", "municipio", "nacionalidade", "natureza_juridica", "ind_portador_defic", "qtd_dias_afastamento", "raca_cor", "regioes_adm_df", "vl_remun_dezembro_nom", "vl_remun_dezembro_sm", "vl_remun_media_nom", "vl_remun_media_sm", "cnae_2_0_subclasse", "sexo_trabalhador", "tamanho_estabelecimento", "tempo_emprego", "tipo_admissao", "tipo_estab", "tipo_estab_1", "tipo_defic", "tipo_vinculo", "ibge_subsetor", "vl_rem_janeiro_cc", "vl_rem_fevereiro_cc", "vl_rem_marco_cc", "vl_rem_abril_cc", "vl_rem_maio_cc", "vl_rem_junho_cc", "vl_rem_julho_cc", "vl_rem_agosto_cc", "vl_rem_setembro_cc", "vl_rem_outubro_cc", "vl_rem_novembro_cc", "ano_chegada_brasil", "ind_trab_intermitente", "ind_trab_parcial"] 
    rais = rais.toDF(*newColumns)

    # Select fewer columns
    rais = rais.select(["cbo_ocupacao_2002", "escolaridade_apos_2005", "qtd_hora_contr", "idade", "mun_trab", "raca_cor", "vl_remun_media_nom", "sexo_trabalhador", "tempo_emprego"])

    # Function to convert commas to dots and turn the vector into Float type
    commaToDot = udf(lambda x : float(str(x).replace(',', '.')), FloatType())

    # Set data type
    rais = rais.withColumn("cbo_ocupacao_2002", rais["cbo_ocupacao_2002"].cast(StringType()))
    rais = rais.withColumn("escolaridade_apos_2005", rais["escolaridade_apos_2005"].cast(StringType()))
    rais = rais.withColumn("qtd_hora_contr", rais["qtd_hora_contr"].cast(IntegerType()))
    rais = rais.withColumn("idade", rais["idade"].cast(IntegerType()))
    rais = rais.withColumn("raca_cor", rais["raca_cor"].cast(StringType()))
    rais = rais.withColumn('vl_remun_media_nom',commaToDot(rais["vl_remun_media_nom"]))
    rais = rais.withColumn("sexo_trabalhador", rais["sexo_trabalhador"].cast(StringType()))
    rais = rais.withColumn('tempo_emprego',commaToDot(rais["tempo_emprego"]))

    # Filter
    rais = rais.filter(rais["cbo_ocupacao_2002"] == "251205")

    rais = rais.toPandas()
    # Cleaned dataset
    print(f"DF {(rais.shape)} created")

    return rais


# Load RAIS data
# http://pdet.mte.gov.br/microdados-rais-e-caged
rais_sul = rais_etl("data/RAIS_VINC_PUB_SUL.txt")
rais_sp = rais_etl("data/RAIS_VINC_PUB_SP.txt")
rais_norte = rais_etl("data/RAIS_VINC_PUB_NORTE.txt")
rais_nordeste = rais_etl("data/RAIS_VINC_PUB_NORDESTE.txt")
rais_mgesrj = rais_etl("data/RAIS_VINC_PUB_MG_ES_RJ.txt")
rais_oeste = rais_etl("data/RAIS_VINC_PUB_CENTRO_OESTE.txt")

# Show us what you've got
# rais_sul.show()

rais_br = pd.concat([rais_sul, rais_sp, rais_norte, rais_nordeste, rais_mgesrj, rais_oeste], ignore_index=True)
rais_br["vl_remun_media_nom"] = round(rais_br["vl_remun_media_nom"], 0)
rais_br.shape
rais_br.dtypes
rais_br.describe()
rais_br.corr()
# rais_br.to_csv("data/rais_br.csv")

rais_br.mun_trab.iloc[:, 1]

# rais_br = rais_sul

4205407 in list(rais_br.mun_trab.iloc[:, 1])
420540 in list(rais_br.mun_trab.iloc[:, 1])
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





