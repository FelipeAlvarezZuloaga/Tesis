import pandas as pd
import numpy as np
import os
from pathlib import Path

class Estandarizador():
    
    def __init__(self, ruta_Agua_acumulada = None, ruta_Bases_de_datos_con_variables = None):
        
        self.ruta_Agua_acumulada = ruta_Agua_acumulada
        self.ruta_Bases_de_datos_con_variables = ruta_Bases_de_datos_con_variables
        
    def Contenido_de_carpeta(self, ruta):
        """Función que dada una ruta de carpeta, retorna la
           lista de rutas, de archivos y carpetas, que contiene

        Returns:
            contenido (list) : Lista que contiene rutas de archivos .txt, string, con la información cruda
                                          del pluviómetro
        """
        archivos = os.listdir(ruta)
        contenido = []
        for archivo in archivos:
            contenido.append((os.path.join(ruta, archivo)))
        return contenido
    
    def Estandariza_agua_acumulada(self, nombre_guardado=None):
        """Función que retorna un archivo .csv que contiene todos los datos de
           la ruta del agua acumulada previamente seleccionada
           
           Se le puede especificar nombre_guardado para cambiar el nombre/ruta del archivo a guardar
        """

        if self.ruta_Agua_acumulada is None:
            return print("No se definió la ruta de la carpeta de agua acumulada")
            
        #Se crea el archivo en el que se va a guardar la información
        cols = ["Fecha", "Agua acumulada PWS100", "Agua acumulada TR525"]
        datos_de_acumulacion = pd.DataFrame(columns = cols)
        if nombre_guardado is not None:
            save = nombre_guardado + ".csv"
        else:
            save = "Datos_de_acumulacion.csv"
        #Si ya existe un archivo con el mismo nombre, se borra
        if os.path.exists(save):
            os.remove(save)
        #Se guarda el archivo nuevo y listo para ser llenado
        datos_de_acumulacion.to_csv(save)
                    
        #Se acceden a todos los archivos en la carpeta del disdrómetro
        contenido = self.Contenido_de_carpeta(self.ruta_Agua_acumulada)
        #Se itera sobre el contnido. 
        for carpeta in contenido:
            #Si el archivo es una carpeta, se busca en su
            #interior, posibles archivos ".txt" que contengan datos
            if os.path.isdir(carpeta):
                datos_txt = self.Contenido_de_carpeta(carpeta)
                #Si algún archivo es un archivo de texto
                #".txt", se extrae la información y se guarda
                for txt in datos_txt:
                    if Path(txt).suffix == '.txt':
                        data = pd.read_csv(txt, sep=" ", header = None)
                        data.to_csv(save, mode='a', header=False)
    
    def Estandariza_datos_con_variables(self, nombre_guardado=None):
        """Función que retorna un archivo .csv que contiene todos los datos de
           la ruta del pluviómetro previamente seleccionada
           
           Se le puede especificar nombre_guardado para cambiar el nombre/ruta del archivo a guardar
        """

        if self.ruta_Bases_de_datos_con_variables is None:
            return print("No se definió la ruta de la carpeta de Base de datos con variables")
        
        #Se crea el archivo en el que se va a guardar la información
        cols = ["Anio", "Mes", "Dia", "Hora", "Minuto", "Segundo", 
                "Tipo de precipitacion", "No. de Gotas", "Intensidad",
                "DTGL 0.05", "DTGL 0.15", "DTGL 0.25", "DTGL 0.35", "DTGL 0.45",
                "DTGL 0.55", "DTGL 0.65", "DTGL 0.75", "DTGL 0.85", "DTGL 0.95",
                "DTGL 1.1", "DTGL 1.3", "DTGL 1.5", "DTGL 1.7", "DTGL 1.9",
                "DTGL 2.2", "DTGL 2.6", "DTGL 3.0", "DTGL 3.4", "DTGL 3.8",
                "DTGL 4.4", "DTGL 5.2", "DTGL 6.0", "DTGL 6.8", "DTGL 7.6",
                "DTGL 8.8", "DTGL 10.4", "DTGL 12.0", "DTGL 13.6", "DTGL 15.2",
                "DTGL 17.6", "DTGL 20.8", "DTGL 24.0", "DTGL 50.0"]        
        datos_con_variables = pd.DataFrame(columns = cols)
        if nombre_guardado is not None:
            save = nombre_guardado + ".csv"
        else:
            save = "Datos_base_con_variables.csv"
        #Si ya existe un archivo con el mismo nombre, se borra
        if os.path.exists(save):
            os.remove(save)
        #Se guarda el archivo nuevo y listo para ser llenado
        datos_con_variables.to_csv(save)
                    
        #Se acceden a todos los archivos en la carpeta del disdrómetro
        contenido = self.Contenido_de_carpeta(self.ruta_Bases_de_datos_con_variables)
        #Se itera sobre el contnido. 
        for txt in contenido:
            #Si algún archivo es un archivo de texto
            #".txt", se extrae la información y se guarda
            if Path(txt).suffix == '.txt':
                data = pd.read_csv(txt, skiprows=2, sep=" ", encoding='latin-1', header = None)
                data.to_csv(save, mode='a', header=False)
    
if __name__=='__main__':
    
    ruta_pluviometro_Chamela = "D:/Base_de_datos_nubes/PWS100_CHAM/Agua_acumulada"
    ruta_disdrometro_Chamela = "D:/Base_de_datos_nubes/PWS100_CHAM/Base_de_datos_con_variables"
    
    Chamela = Estandarizador(ruta_pluviometro_Chamela, ruta_disdrometro_Chamela)
    Chamela.Estandariza_agua_acumulada("Acumulado_Chamela_crudo")
    Chamela.Estandariza_datos_con_variables("Datos_con_variables_Chamela_crudo")
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    