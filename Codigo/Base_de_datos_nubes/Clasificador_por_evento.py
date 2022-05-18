import pandas as pd
import numpy as np
import os
from datetime import datetime
from pandas import Timedelta
import plotly.express as px
import toml
import warnings
warnings.filterwarnings("ignore")


class Generador_de_Eventos():
    def __init__(self, ruta_config = None, ruta_Agua_acumulada_limpia = None, ruta_Bases_de_datos_con_variables_estandarizada = None):
        if ruta_config is not None:
            self.config = inicializa_configuraciones(ruta_config)
            self.Tiempo_de_consideracion_mismo_evento_min = self.config["Tiempo_de_consideracion_mismo_evento_min"]
            self.Intensidad_de_precipitacion_minima_consideracion_lluvia_mm_h = self.config["Intensidad_de_precipitacion_minima_consideracion_lluvia_mm_h"]
            self.Duracion_total_minima_de_evento_min = self.config["Duracion_total_minima_de_evento_min"]
            self.Intensidad_de_precipitacion_promedio_minima_mm_h = self.config["Intensidad_de_precipitacion_promedio_minima_mm_h"]
            
        self.ruta_Agua_acumulada_limpia = ruta_Agua_acumulada_limpia        
        self.ruta_Bases_de_datos_con_variables_estandarizada = ruta_Bases_de_datos_con_variables_estandarizada

    def Genera_eventos(self, nombre_guardado=None):
        """Función que separa la base de datos por eventos dado 
        los parámetros en config 
        Se le puede dar un nombre específico para guardar la base separada
        """
        datos_por_evento = pd.read_csv(self.ruta_Bases_de_datos_con_variables_estandarizada)
        #Cambiamos las fehcas del dataframe a formato Datetime para poder operar con mayor facilidad
        datos_por_evento["Fecha"] = datos_por_evento["Fecha"].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))  
        datos_por_evento = datos_por_evento.set_index("Fecha")

        #Creamos la columna de eventos
        datos_por_evento["Evento"] = 0
        #Iniciamos algunas consantes
        evento = 1    
        hora_fin_evento = datos_por_evento.index[0]   
        #Iniciamos como variable el criterio de espera de tiempo
        delta = Timedelta(minutes=self.Tiempo_de_consideracion_mismo_evento_min)
        agua_min = self.Intensidad_de_precipitacion_minima_consideracion_lluvia_mm_h
        #Iteramos sobre todos los minutos
        for minuto in range(datos_por_evento.shape[0]):
            
            #Si ha pasado "delta" tiempo desde la creación del último evento de precipitación, es un nuevo evento
            if (datos_por_evento.index[minuto] - hora_fin_evento) > delta:
                evento += 1 #Actualizamos la etiqueta de evento
                hora_fin_evento = datos_por_evento.index[minuto] #Actualizamos la hora de "fin" del evento nuevo
            
            #Si no ha pasado el tiempo suficiente, checamos el agua
            else:
                if datos_por_evento["Intensidad"].iloc[minuto] <= agua_min:
                    #Si la intensidad de precipitación es mayor a lo considerado, entonces se actualiza la hora de "fin" del evento nuevo
                    hora_fin_evento = datos_por_evento.index[minuto] #Actualizamos la hora de "fin" del evento
                else:
                    #En caso de que no llegue a lo minimo no actualizamos el "fin" del evento
                    pass
                
            #Actualizamos el evento en la columna
            datos_por_evento["Evento"].iloc[minuto] = evento                       

        #Se guarda el archivo
        if nombre_guardado is not None:
            save = nombre_guardado + ".csv"
        else:
            save = "Datos_por_evento_tiempo_" + str(self.Tiempo_de_consideracion_mismo_evento_min) + "_precipitacionminima_" + str(self.Intensidad_de_precipitacion_minima_consideracion_lluvia_mm_h) + ".csv"
        #Si ya existe un archivo con el mismo nombre, se borra
        if os.path.exists(save):
            os.remove(save)
        #Se guarda el archivo nuevo y listo para ser llenado
        datos_por_evento.to_csv(save)
        
    def Genera_resumen(self, ruta_df_con_eventos=None ,nombre_guardado=None):
        """Función que genera un resumen de los diferentes eventos generados
        """
        #Creamos un dataframe que contiene las siguientes columnas:
        cols = ["Evento", "Fecha de inicio", "Fecha de fin", "Duracion del evento",
                "Intensidad de precipitacion promedio"]
        
        resumen = pd.DataFrame(columns = cols)
        
        #Leemos el .csv con los eventos clasificados
        if ruta_df_con_eventos is None:
            ruta_df_con_eventos = "Datos_por_evento_tiempo_" + str(self.Tiempo_de_consideracion_mismo_evento_min) + "_precipitacionminima_" + str(self.Intensidad_de_precipitacion_minima_consideracion_lluvia_mm_h) + ".csv"

        datos_por_evento = pd.read_csv(ruta_df_con_eventos)
        datos_por_evento["Fecha"] = datos_por_evento["Fecha"].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))  
        
        #Iteramos sobre todos los eventos
        for i in datos_por_evento["Evento"].unique(): 
            evs = np.where(datos_por_evento["Evento"] == i)
            #Generamos los datos de la fila del evento
            ini = evs[0][0]
            fin = evs[0][-1]
            Evento = i
            Fecha_inicio = datos_por_evento["Fecha"][ini]
            Fecha_fin = datos_por_evento["Fecha"][fin]
            Duracion = int(((Fecha_fin - Fecha_inicio).seconds) / 60) + 1
            Intensidad_promedio = (datos_por_evento["Intensidad"][ini : fin+1].sum())/Duracion
            #Guardamos la información en un nuevo dataframe
            apendar = {"Evento" : [Evento], "Fecha de inicio" : [Fecha_inicio], "Fecha de fin" : [Fecha_fin], "Duracion del evento" : [Duracion], 
                       "Intensidad de precipitacion promedio" : [Intensidad_promedio]}
            apendar = pd.DataFrame(apendar)
            resumen = pd.concat([resumen, apendar], axis=0)
        
        #Se guarda el archivo
        if nombre_guardado is not None:
            save = nombre_guardado + ".csv"
        else:
            save = "Resumen_de_eventos_" + str(self.Tiempo_de_consideracion_mismo_evento_min) + "_precipitacionminima_" + str(self.Intensidad_de_precipitacion_minima_consideracion_lluvia_mm_h) + ".csv"
        #Si ya existe un archivo con el mismo nombre, se borra
        if os.path.exists(save):
            os.remove(save)
        #Se guarda el archivo nuevo y listo para ser llenado
        resumen.to_csv(save)

class Clasificador_de_Eventos():
    def __init__(self,  ruta_config = None, Ruta_eventos = None, Ruta_resumen = None):
        if ruta_config is not None:
            self.config = inicializa_configuraciones(ruta_config)
            self.Tiempo_de_consideracion_mismo_evento_min = self.config["Tiempo_de_consideracion_mismo_evento_min"]
            self.Intensidad_de_precipitacion_minima_consideracion_lluvia_mm_h = self.config["Intensidad_de_precipitacion_minima_consideracion_lluvia_mm_h"]
            self.Duracion_total_minima_de_evento_min = self.config["Duracion_total_minima_de_evento_min"]
            self.Intensidad_de_precipitacion_promedio_minima_mm_h = self.config["Intensidad_de_precipitacion_promedio_minima_mm_h"]
        self.Ruta_eventos = Ruta_eventos   
        self.Ruta_resumen = Ruta_resumen
        
    def Clasifica_eventos_por_fecha(self, grafica=True, ruta_madre=None):
        """Función que separa la base de datos por
        eventos, en una carpeta con el nombre de ruta_madre, en carpetas
        con fechas junto con gráficas de precipitación acumulada
        Se le puede dar un nombre específico para guardar la base separada
        """
        #Transformamos en formato tiempo las fechas del csv de resumenes
        resumen_ev = pd.read_csv(self.Ruta_resumen)
        resumen_ev["Fecha de inicio"] = resumen_ev["Fecha de inicio"].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))  
        resumen_ev["Fecha de fin"] = resumen_ev["Fecha de fin"].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))  
        try:
            resumen_ev = resumen_ev.drop(["Unnamed: 0"], 1)
        except:
            pass
        
        #Hacemos lo análogo con los eventos crudos
        datos_por_evento = pd.read_csv(self.Ruta_eventos)
        datos_por_evento["Fecha"] = datos_por_evento["Fecha"].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')) 
        datos_por_evento = datos_por_evento.set_index('Fecha')
        try:
            datos_por_evento = datos_por_evento.drop(["Unnamed: 0"], 1)
        except:
            pass 
        
        #Filtramos los eventos por duracion e intensidad de precipitacion y guardamos
        resumen_ev = resumen_ev[resumen_ev["Duracion del evento"]>=self.Duracion_total_minima_de_evento_min]        
        resumen_ev = resumen_ev[resumen_ev["Intensidad de precipitacion promedio"]>=self.Intensidad_de_precipitacion_promedio_minima_mm_h]

        filtrados = "Datos_filtrados_T_min_" + str(self.Duracion_total_minima_de_evento_min) + "_precipitacionminimapromedio_" + str(self.Intensidad_de_precipitacion_minima_consideracion_lluvia_mm_h) + ".csv"
        resumen_ev.to_csv(filtrados)

        resumen_ev = resumen_ev.reset_index(drop=True)
        
        #Creamos una carpeta para guardar todo lo que se va a generar utilizando el nombre de ruta madre
        if ruta_madre is None:
            ruta_madre = "Eventos_clasificados"
        if os.path.exists(ruta_madre):
            os.rmdir(ruta_madre)
        os.mkdir(ruta_madre)
        
        #Creamos un dataframe auxiliar que contiene únicamente los años y meses de cada evento de precipitación
        df = pd.DataFrame()
        df['A ini'] = pd.DatetimeIndex(resumen_ev["Fecha de inicio"]).year
        df['M ini'] = pd.DatetimeIndex(resumen_ev["Fecha de inicio"]).month 
        
        #Iteramos para cada evento
        for evento in range(resumen_ev.shape[0]):
            #Extraemos el mes y anio del evento
            anio = str(df['A ini'][evento])
            mes = str(df['M ini'][evento])
            ruta_a = ruta_madre + "/" + anio 
            #Revisamos si existe la carpeta del año
            if not os.path.exists(ruta_a):
                os.mkdir(ruta_a)
            ruta_m = ruta_a + "/" + mes
            #Revisamos si existe la carpeta del mes
            if not os.path.exists(ruta_m):
                os.mkdir(ruta_m)
            
            #Guardamos el resumen del evento
            ev_particular = resumen_ev[evento:evento+1]
            if os.path.exists(ruta_m + "/" + "resumen_eventos.csv"):
                ev_particular.to_csv(ruta_m + "/" + "resumen_eventos.csv", mode='a', header=False)
            if not os.path.exists(ruta_m + "/" + "resumen_eventos.csv"):
                ev_particular.to_csv(ruta_m + "/" + "resumen_eventos.csv", mode='a', header=True)
                
            #Guardamos el evento
            inicio = resumen_ev["Fecha de inicio"][evento]
            fin = resumen_ev["Fecha de fin"][evento]
            ev_df = datos_por_evento[inicio : fin]
            if os.path.exists(ruta_m + "/" + "datos_crudos_eventos.csv"):
                ev_df.to_csv(ruta_m + "/" + "datos_crudos_eventos.csv", mode='a', header=False)
            if not os.path.exists(ruta_m + "/" + "datos_crudos_eventos.csv"):
                ev_df.to_csv(ruta_m + "/" + "datos_crudos_eventos.csv", mode='a', header=True)
            
            #Si se quiere guardar las gráficas de la precipitación acumulada, se hace
            if grafica is True:
                fig = px.bar(ev_df, x=ev_df.index, y = "Intensidad", title='Intensidad de precipitacion del evento ' + str(evento + 1), text="Intensidad")
                fig.update_layout(
                yaxis_title=r'$ \text{Intensidad de precipitacion } [mm h^{-1}]$')
                fig.write_image(ruta_m + "/" + "Evento_" + str(evento + 1) + ".png")

def inicializa_configuraciones(ruta_config):
    """Función que lee el archivo de configuraciones ".toml"
    dada la ruta ruta_config

    Tiempo_de_consideracion_mismo_evento_min -> Es el tiempo, en minutos, que se debe tener registrado con
    nula precipitación, o registro nulo, para considerar un evento finalizado   

    Duracion_total_minima_de_evento_min -> Es un filtro pensado para obtener únicamente eventos de larga duración

    Intensidad_de_precipitacion_promedio_minima_mm_h -> Es un filtro pensado para obtener únicamente eventos donde hubo intensas lluvias

    Returns:
        config (Dict) : El diccionario con las configuraciones
    """
    #Se carga el archivo
    tom = toml.load(ruta_config)
    #Se ingresa al diccionario de los diferentes apartados
    eventos = tom["Eventos"]
    filtros = tom["Filtros"]

    config = {'Tiempo_de_consideracion_mismo_evento_min': eventos['Tiempo_de_consideracion_mismo_evento_min'],
              "Intensidad_de_precipitacion_minima_consideracion_lluvia_mm_h" : eventos["Intensidad_de_precipitacion_minima_consideracion_lluvia_mm_h"],
              "Duracion_total_minima_de_evento_min" : filtros['Duracion_total_minima_de_evento_min'],
              "Intensidad_de_precipitacion_promedio_minima_mm_h" :  filtros['Intensidad_de_precipitacion_promedio_minima_mm_h']
              }

    return config

if __name__ == '__main__':
    
    
    ruta_acumulado_Chamela = ""
    ruta_variables_Chamela = "D:/Base_de_datos_nubes/Datos_con_variables_Chamela_limpio.csv"
    ruta_config = "D:/Base_de_datos_nubes/config_eventos.toml"
    ruta_eventos = "D:/Base_de_datos_nubes/Datos_por_evento_tiempo_5_precipitacionminima_0.1.csv"  
    ruta_resumen = "D:/Base_de_datos_nubes/Resumen_de_eventos_5_precipitacionminima_0.1.csv"
    
    config = inicializa_configuraciones(ruta_config)
    
    Chamela = Generador_de_Eventos(ruta_config, ruta_acumulado_Chamela, ruta_variables_Chamela)
    Chamela.Genera_eventos()
    Chamela.Genera_resumen()
    
    Chamela_genera = Clasificador_de_Eventos(ruta_config, ruta_eventos, ruta_resumen)
    Chamela_genera.Clasifica_eventos_por_fecha()

    
    
    
    
    