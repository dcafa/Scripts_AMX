# <<NOTAS>>
# script testeado en forma local
# el script recibe como argumento el archivo de configuracion con la informacion del dataset a procesar
# dataset delimitado: config_file1.ini
# dataset ancho fijo: config_file2.ini

# modo de ejecucion:
# > spark-submit spark_process.py --config_file_name=<config_file1.ini>


from pyspark.sql import SparkSession,functions as F
import configparser
import datetime
import argparse
import os
import sys

class configParser():
    '''
    levanta los parametros de configuracion necesarios para procesar el dataset
    '''
    def __init__(self, config):
        #path_origen
        self.path_origen = config['path_origen']['path']

        #path_destino
        self.path_destino = config['path_destino']['path']

        #metadata_input
        self.delimitador = config['metadata_input']['delimitador']
        self.nombre_columnas = config['metadata_input']['nombre_columnas']
        self.tipo_dato_columnas = config['metadata_input']['tipo_dato_columnas']
        self.ancho_columnas = config['metadata_input']['ancho_columnas']
        self.ts_pattern = config['metadata_input']['ts_pattern']

        #metadata_output
        self.particiones = config['metadata_output']['particiones']
        self.cantidad_archivos = config['metadata_output']['cantidad_archivos']
        self.compresion = config['metadata_output']['compresion']
        self.cols_to_add = config['metadata_output']['cols_to_add']
        self.cols_to_add_values = config['metadata_output']['cols_to_add_values']
        self.partition_date = config['metadata_output']['partition_date']

def init_argparse() -> argparse.ArgumentParser:
    """
    obtiene el nombre del archivo de configuracion necesario para procesar el dataset
    """
    parser = argparse.ArgumentParser(
        usage="%(prog)s [--config_file_name]",
        description="pyspark app."
    )

    parser.add_argument('--config_file_name',
                        metavar='config_file_name',
                        type=str,
                        help='config_file_name',
                        action='store',
                        #nargs='*'
                        )

    parser.add_argument(
        "-v", "--version", action="version",
        version = f"{parser.prog} version 1.0.0"
    )

    return parser

def save_parquet(df,output_filename):
    """
    exporta el dataset procesado en formato parquet
    """
    df.repartition(int(cp.cantidad_archivos)).write.partitionBy(partitions).mode("overwrite") \
            .option("compression", "{}".format(cp.compresion)).parquet(cp.path_destino + '/' + output_filename)

def func_cols_to_add(df):
    """
    realiza el agregado de columnas con valores fijo dadas por parametro
    """
    cols_to_add = cp.cols_to_add.split(',')
    cols_to_add_values = cp.cols_to_add_values.split(',')
    for i,col in enumerate(cols_to_add):
        df = df.withColumn(col, F.lit(cols_to_add_values[i]))
    
    return df

if __name__ == "__main__":
    
    parser = init_argparse()
    arg = parser.parse_args()
    if (arg.config_file_name is None):
        print("error. Se tiene que indicar el archivo de configuracion. se sale")
        sys.exit(1)

    current_dir = os.path.dirname(__file__) 
    config = configparser.ConfigParser(allow_no_value=True)
    config.read(current_dir + '/' + '{}'.format(arg.config_file_name))
    cp = configParser(config)
    dataset_path = cp.path_origen
    partitions  = cp.particiones.split(',')

    session = SparkSession.builder.appName("appPyspark").master("local[*]").getOrCreate()    

    # --------------------------------
    # levantamos el dataset delimitado
    # --------------------------------
    if cp.delimitador is not None:
        df = session.read.option("delimiter",cp.delimitador).csv(header=True, inferSchema=True, path=dataset_path)
        
        #al dataset delimitado le agregamos la col partition_date indicada como parametro
        df = df.withColumn("partition_date", F.lit(cp.partition_date))
        partitions.append('partition_date')    

        df = func_cols_to_add(df)
        
        # ------------
        # save parquet
        # ------------
        output_filename = 'delimitado.parquet'
        save_parquet(df,output_filename)
            
    # -----------------------------------
    # levantamos el dataset de ancho fijo
    # -----------------------------------
    else:
        nombre_columnas = cp.nombre_columnas.split(',')
        ancho_columnas = cp.ancho_columnas.split(',')
        tipo_datos_columnas = cp.tipo_dato_columnas.split(',')
        ts_pattern = cp.ts_pattern

        df = session.read.text(dataset_path)

        # -----------------------------
        # setting del ancho de columnas
        # -----------------------------
        _ancho_columnas = [1,*ancho_columnas]         
        def calc_acc(i):
            return sum([int(i) for i in _ancho_columnas[0:i]])

        acc = [int(ancho) + calc_acc(i) for i,ancho in enumerate(_ancho_columnas[:-1])]
        tups = list(zip(acc,ancho_columnas))
        df = df.select(
            [df.value.substr(int(tup[0]),int(tup[1])).alias(nombre_columnas[i]) for i,tup in enumerate(tups)]
        )

        # -------------------------
        # setting del tipo de datos
        # -------------------------
        for i in range(len(nombre_columnas)):
            #tratamiento particular para col timestamp
            if tipo_datos_columnas[i] == 'timestamp':
                df = df.withColumn(nombre_columnas[i], F.to_timestamp(df[nombre_columnas[i]],ts_pattern))
            else:
                df = df.withColumn(nombre_columnas[i], F.col(nombre_columnas[i]).cast(tipo_datos_columnas[i]))

        # ------------
        # save parquet
        # ------------
        output_filename = 'ancho.fijo.parquet'
        save_parquet(df,output_filename)

    df.show()
    #print(df.printSchema())
    session.stop()