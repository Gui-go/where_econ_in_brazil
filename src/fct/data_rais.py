#!/usr/bin/env python3

"""
Python function to transform the RAIS data and filter it to the 251205 CBO, Economists.
"""

import pyspark
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, udf
from pyspark.sql.types import FloatType, StringType, IntegerType


def rais_etl(files: list) -> pd.DataFrame:
    """
    This function filter, transform and return RAIS data as a pandas DataFrame.
    INPUT:
        files -> list (local addresses to the RAIS files)
    Output:
        df -> pd.DataFrame (all economists data from RAIS)
    """

    # Start Spark session
    spark = SparkSession.builder.appName('spark_session').getOrCreate()

    rais_lista = list()
    
    for file in files:
        # Load RAIS
        rais_df = spark.read.load(file, format="csv", sep=";", inferSchema="true", header="true")
    
        # Change column names
        newColumns = ["bairros_sp", "bairros_fortaleza", "bairros_rj", "causa_afastamento_1", "causa_afastamento_2", "causa_afastamento_3", "motivo_desligamento", "cbo_ocupacao_2002", "cnae_2_0_classe", "cnae_95_classe", "distritos_sp", "vinculo_ativo_31_12", "faixa_etaria", "faixa_hora_contrat", "faixa_remun_dezem_sm", "faixa_remun_media_sm", "faixa_tempo_emprego", "escolaridade_apos_2005", "qtd_hora_contr", "idade", "ind_cei_vinculado", "ind_simples", "mes_admissao", "mes_desligamento", "mun_trab", "municipio", "nacionalidade", "natureza_juridica", "ind_portador_defic", "qtd_dias_afastamento", "raca_cor", "regioes_adm_df", "vl_remun_dezembro_nom", "vl_remun_dezembro_sm", "vl_remun_media_nom", "vl_remun_media_sm", "cnae_2_0_subclasse", "sexo_trabalhador", "tamanho_estabelecimento", "tempo_emprego", "tipo_admissao", "tipo_estab", "tipo_estab_1", "tipo_defic", "tipo_vinculo", "ibge_subsetor", "vl_rem_janeiro_cc", "vl_rem_fevereiro_cc", "vl_rem_marco_cc", "vl_rem_abril_cc", "vl_rem_maio_cc", "vl_rem_junho_cc", "vl_rem_julho_cc", "vl_rem_agosto_cc", "vl_rem_setembro_cc", "vl_rem_outubro_cc", "vl_rem_novembro_cc", "ano_chegada_brasil", "ind_trab_intermitente", "ind_trab_parcial"] 
        rais_df = rais_df.toDF(*newColumns)

        # Select fewer columns
        rais_df = rais_df.select(["cbo_ocupacao_2002", "escolaridade_apos_2005", "qtd_hora_contr", "idade", "mun_trab", "raca_cor", "vl_remun_media_nom", "sexo_trabalhador", "tempo_emprego"])

        # Filter
        rais_df = rais_df.filter(rais_df["cbo_ocupacao_2002"] == "251205")

        # Function to convert commas to dots and turn the vector into Float type
        commaToDot = udf(lambda x : float(str(x).replace(',', '.')), FloatType())

        # Set data type
        rais_df = rais_df.withColumn("cbo_ocupacao_2002", rais_df["cbo_ocupacao_2002"].cast(StringType()))
        rais_df = rais_df.withColumn("escolaridade_apos_2005", rais_df["escolaridade_apos_2005"].cast(StringType()))
        rais_df = rais_df.withColumn("mun_trab", rais_df["mun_trab"].cast(StringType()))
        rais_df = rais_df.withColumn("qtd_hora_contr", rais_df["qtd_hora_contr"].cast(IntegerType()))
        rais_df = rais_df.withColumn("idade", rais_df["idade"].cast(IntegerType()))
        rais_df = rais_df.withColumn("raca_cor", rais_df["raca_cor"].cast(StringType()))
        rais_df = rais_df.withColumn('vl_remun_media_nom',commaToDot(rais_df["vl_remun_media_nom"]))
        rais_df = rais_df.withColumn("sexo_trabalhador", rais_df["sexo_trabalhador"].cast(StringType()))
        rais_df = rais_df.withColumn('tempo_emprego',commaToDot(rais_df["tempo_emprego"]))

        # Turn it into a pandas DataFrame
        rais_df = rais_df.toPandas()

        # Append each transformed sample
        rais_lista.append(rais_df)
    
    # Concat them all
    rais_br = pd.concat(rais_lista, ignore_index=True)

    # Turn vl_remun_media_nom into integer
    rais_br["vl_remun_media_nom"] = round(rais_br["vl_remun_media_nom"], 0)

    # Stopping session
    spark.stop()

    # End call
    print(f"DF {(rais_br.shape)} created")

    return rais_br

