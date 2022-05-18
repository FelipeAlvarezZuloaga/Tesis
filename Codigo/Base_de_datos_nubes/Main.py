from Estandarizador import Estandarizador
from Preprocesador import Preprocesador
from Clasificador_por_evento import Generador_de_Eventos, Clasificador_de_Eventos, inicializa_configuraciones

import pandas as pd
import numpy as np
import os
from datetime import datetime
from pandas import Timedelta
import plotly.express as px
from pathlib import Path
import time
import shutil
import toml
import pprint
import warnings
warnings.filterwarnings("ignore")

#Define la ruta base en la que se encuentran los archivos .py así como las carpetas con datos crudos
ruta_base = "D:/Base_de_datos_nubes/"

#Definimos las estaciones que queremos procesar junto con la ruta, puede ser relativa, de los
#archivos crudos de cada una
estaciones = ["Chamela", "Juriquilla", "Los_Tuxtlas"]
rutas = ["PWS100_CHAM", "PWS100_JQRO", "PWS100_LTUX"]

#Asignamos la ruta del archivo de configuraciones e imprimimos las configuraciones iniciales
ruta_config = ruta_base + "config_eventos.toml"    
config = inicializa_configuraciones(ruta_config)
pprint.PrettyPrinter(depth=2).pprint(config)

#Haremos el mismo procesamiento para cada estación
for i in range(len(estaciones)):
    #Tiempo de ejecución
    start_time = time.time()
    #Inicialización de variables
    estacion = estaciones[i]
    ruta = rutas[i]
    #Ruta de los datos crudos
    ruta_pluviometro = ruta_base + ruta + "/Agua_acumulada"
    ruta_disdrometro = ruta_base + ruta + "/Base_de_datos_con_variables"
    print("Estación ", estacion, " iniciada")
    
    #Estandariza los datos crudos
    nombre_agua_acumulada_crudo = "Acumulado_" + estacion + "_crudo"
    nombre_disdrometro_crudo = "Datos_con_variables_" + estacion + "_crudo"
    Estandariza = Estandarizador(ruta_pluviometro, ruta_disdrometro)
    Estandariza.Estandariza_agua_acumulada(nombre_agua_acumulada_crudo)
    Estandariza.Estandariza_datos_con_variables(nombre_disdrometro_crudo)
    print("Estandarización finalizada")
    print("--- %s segundos ---" % (time.time() - start_time))
    
    #Preprocesa los archivos que se acaban de crear
    nombre_agua_acumulada_limpio = "Acumulado_" + estacion + "_limpio"
    nombre_disdrometro_limpio = "Datos_con_variables_" + estacion + "_limpio"  
    Preprocesa = Preprocesador(ruta_base + nombre_agua_acumulada_crudo + ".csv", ruta_base + nombre_disdrometro_crudo+ ".csv")
    Preprocesa.Limpiar_Agua_acumulada(nombre_agua_acumulada_limpio)
    Preprocesa.Limpiar_Datos_con_varables(nombre_disdrometro_limpio)
    print("Procesamiento finalizado")
    print("--- %s segundos ---" % (time.time() - start_time))
    
    #Clasificamos en eventos de precipitación
    ruta_acumulado_Chamela = ruta_base + nombre_agua_acumulada_limpio
    ruta_variables_Chamela = ruta_base + nombre_disdrometro_limpio    
    Generador = Generador_de_Eventos(ruta_config, ruta_acumulado_Chamela + ".csv", ruta_variables_Chamela + ".csv")
    Generador.Genera_eventos()
    Generador.Genera_resumen()
    print("Clasificación finalizada")
    print("--- %s segundos ---" % (time.time() - start_time))
    
    #Filtramos los eventos de precipitación e incluimos las gráficas
    ruta_eventos = ruta_base + "Datos_por_evento_tiempo_" + str(config["Tiempo_de_consideracion_mismo_evento_min"]) + "_precipitacionminima_"+ str(config["Intensidad_de_precipitacion_minima_consideracion_lluvia_mm_h"]) +".csv"  
    ruta_resumen = ruta_base + "Resumen_de_eventos_" + str(config["Tiempo_de_consideracion_mismo_evento_min"]) + "_precipitacionminima_" + str(config["Intensidad_de_precipitacion_minima_consideracion_lluvia_mm_h"]) + ".csv"
    Clasificador = Clasificador_de_Eventos(ruta_config, ruta_eventos, ruta_resumen)
    Clasificador.Clasifica_eventos_por_fecha()
    print("Filtración y graficación finalizada")
    print("--- %s segundos ---" % (time.time() - start_time))
    
    print("Estación ", estacion, " terminada")
     
    #Movemos todo lo creado a una carpeta para tener todo en orden
    os.mkdir(ruta_base + estacion)
    
    #Tomamos e contenido de la ruta actual
    archivos = os.listdir(ruta_base)
    contenido = []
    for archivo in archivos:
        contenido.append((os.path.join(ruta_base, archivo)))
    #Se itera sobre el contnido. 
    for csv in contenido:
        #Si algún archivo es un archivo 
        #".csv", se mueve
        if Path(csv).suffix == '.csv':
            shutil.move(csv, ruta_base + estacion)    
    #Finalmente, movemos la última carpeta
    shutil.move("Eventos_clasificados", ruta_base + estacion)   
    