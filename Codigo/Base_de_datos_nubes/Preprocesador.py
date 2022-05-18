import pandas as pd
import numpy as np
import os
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

class Preprocesador():
    
    def __init__(self, ruta_Agua_acumulada_estandarizada = None, ruta_Bases_de_datos_con_variables_estandarizada = None):
        self.ruta_Agua_acumulada_estandarizada = ruta_Agua_acumulada_estandarizada
        self.ruta_Bases_de_datos_con_variables_estandarizada = ruta_Bases_de_datos_con_variables_estandarizada
        
    def Incluir_fechas_Agua_acumulada(self):
        """Función que cambia la columna de fechas del
        dataframe Acumulado estandarizado, y las convierte
        en un indice como Timestamp
        
        Returns:
            data (pd.DataFrame) : El dataframe con fechas como índice
        """
        #Se lee el csv acumulado
        data = pd.read_csv(self.ruta_Agua_acumulada_estandarizada)
        #Se dropea la columna Unnamed: 0, si es que se creó
        try:
            data = data.drop(["Unnamed: 0"], 1)
        except:
            pass
        
        #Intenta hacer el cambio de fehca, puede que no se haga  porque ya se habia hecho anteriormente
        try:
            #Cambiamos las fehcas del dataframe a formato Datetime para poder operar con mayor facilidad
            data["Fecha"] = data["Fecha"].apply(lambda x: datetime.strptime(x, '%Y%m%d-%H%M%S'))
            #Colocamos las fechas como índice para tener acceso rápido a estas
            data = data.set_index('Fecha')
        except:
            pass
        return data
                    
    def Quita_NA(self, data):
        """Función que quita todos los datos NAN
        de un dataframe
        """
        return data.dropna()
    
    def Limpiar_Agua_acumulada(self,  nombre_guardado=None):
        """Función que limpia y deja listo la base
        de agua acumulada y la deja lista para trabajar
        Se le puede incluir un nombre de guardado especial
        
        """
        datos_acumulada = self.Incluir_fechas_Agua_acumulada()
        datos_acumulada = self.Quita_NA(datos_acumulada)
        
        #Se guarda el archivo
        if nombre_guardado is not None:
            save = nombre_guardado + ".csv"
        else:
            save = "Datos_acumulados.csv"
        #Si ya existe un archivo con el mismo nombre, se borra
        if os.path.exists(save):
            os.remove(save)
        #Se guarda el archivo nuevo y listo para ser llenado
        datos_acumulada.to_csv(save)
                
    def Incluir_fechas_Datos_con_Variables(self):
        """Función que cambia la columna de fechas del
        dataframe Bases de datos con variables estandarizado
        y las convierte en un indice como Timestamp
        
        Returns:
            data (pd.DataFrame) : El dataframe con fechas como índice
        """
        #Se lee el csv acumulado
        data = pd.read_csv(self.ruta_Bases_de_datos_con_variables_estandarizada)
        
        #Se quita la columna Unnamed: 0, si es que se creó
        try:
            data = data.drop(["Unnamed: 0"], 1)
        except:
            pass
        
        #Intenta hacer el cambio de fehca. Puede que no se haga porque ya se habia hecho anteriormente
        try:
            #Hacemos una columna uniendo las columnas de fechas       
            cols_str = ["Anio", "Mes", "Dia", "Hora", "Minuto", "Segundo"]
            for col in cols_str:
                data[col] = data[col].apply(lambda x: str(x))
            data["Fecha"] = data["Anio"] + "-" + data["Mes"] + "-" + data["Dia"] + "-" +data["Hora"] + "-" + data["Minuto"] + "-" +data["Segundo"]  
            #Cambiamos las fehcas del dataframe a formato Datetime para poder operar con mayor facilidad
            data["Fecha"] = data["Fecha"].apply(lambda x: datetime.strptime(x, '%Y-%m-%d-%H-%M-%S'))
            #Colocamos las fechas como índice para tener acceso rápido a estas
            data = data.set_index('Fecha')
            #Quitamos las columnas que se usaron para construir la fecha
            data = data.drop(cols_str, 1)
        except:
            pass
        
        return data

    def Limpiar_Datos_con_varables(self,  nombre_guardado=None):
        """Función que limpia y deja listo la base
        de datos con variales y la deja lista para trabajar
        Se le puede incluir un nombre de guardado especial
        
        """
        datos_con_variables = self.Incluir_fechas_Datos_con_Variables()
        datos_con_variables = self.Quita_NA(datos_con_variables)
        
        #Para el caso de la base de datos con variables, se incluyeron resultados 
        #con -999 en los tamños de gota que no son validos para procesar. 
        # Por lo tanto, quitaremos esos resultados también
        datos_con_variables = datos_con_variables[datos_con_variables['DTGL 0.05'] > -999]
        
        #Se guarda el archivo
        if nombre_guardado is not None:
            save = nombre_guardado + ".csv"
        else:
            save = "Datos_base_con_variables.csv"
        #Si ya existe un archivo con el mismo nombre, se borra
        if os.path.exists(save):
            os.remove(save)
        #Se guarda el archivo nuevo y listo para ser llenado
        datos_con_variables.to_csv(save)
        
        return 
   
if __name__=='__main__':
    
    ruta_acumulado_Chamela = "D:/Base_de_datos_nubes/Acumulado_Chamela_crudo.csv"
    ruta_variables_Chamela = "D:/Base_de_datos_nubes/Datos_con_variables_Chamela_crudo.csv"
    
    Chamela = Preprocesador(ruta_acumulado_Chamela, ruta_variables_Chamela)
    Chamela.Limpiar_Agua_acumulada("Acumulado_Chamela_limpio")
    Chamela.Limpiar_Datos_con_varables("Datos_con_variables_Chamela_limpio")
    







