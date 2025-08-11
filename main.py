import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import re
import zipfile
import shutil
from azure.storage.filedatalake import DataLakeServiceClient

ruta_destino_archivos_parquet = "archivos_parquet"

ruta_destino_PSUF_datalake = f"Prueba/PSUF/data"
ruta_destino_PEST_datalake = f"Prueba/PEST_NUEVO/data"
ruta_destino_SSCC_datalake = f"Prueba/SSCC/data"
ruta_destino_CVF_datalake = f"Prueba/CVF/data"
ruta_destino_Asignacion_Dx_datalake = f"Prueba/Dx/data"

dic_rutas_datalake = {
    'PSUF': ruta_destino_PSUF_datalake,
    'PEST': ruta_destino_PEST_datalake,
    'SSCC': ruta_destino_SSCC_datalake,
    'CVF': ruta_destino_CVF_datalake,
    'AsignacionDx': ruta_destino_Asignacion_Dx_datalake
    }


def initialize_storage_account_sas(storage_account_name, sas_token: str):

    try:  

        global service_client
        service_client = DataLakeServiceClient(
            account_url=f"https://{storage_account_name}.dfs.core.windows.net",
            credential=sas_token
        )

    except Exception as e:
        
        print(f"Error inicializando el cliente: {e}")
        raise
 
def cargar_archivo(ruta, local_file_path, nombre_archivo):

    try:
       
        file_system_client = service_client.get_file_system_client(file_system="grupo-cerro-uno-files")
        
        directory_client = file_system_client.get_directory_client(ruta)

        if not directory_client.exists():

            directory_client.create_directory()
            print(f"Directorio '{ruta}' creado exitosamente")
        
        file_client = directory_client.create_file(nombre_archivo)
        
        with open(local_file_path, "rb") as local_file:

            file_contents = local_file.read()
            file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
            file_client.flush_data(len(file_contents))
            
        print(f"Archivo {nombre_archivo} subido exitosamente a {ruta}")

    except Exception as e:

        print(f"Error subiendo archivo: {e}")
        raise


def descomprimir_Zip():

    ruta_zips = "zips/"

    if not os.path.exists(ruta_zips):

        print("La carpeta zips no existe, por favor crear la carpeta y cargar los archivos correspondientes.")
        return None

    patron = r'PLABACOM_(\d{4})_(\d{2})_.*?(Potencia|SSCC|Energia).*?(Definitivo|Def|DEF|def).*\.zip$'
    carpetas = ["Energia", "SSCC", "Potencia"]
    for carpeta in carpetas:

        os.makedirs(carpeta, exist_ok=True)
    
    os.makedirs(ruta_destino_archivos_parquet, exist_ok=True)

    anio_esperado = None
    mes_esperado = None
    version = None
    archivos_procesados = 0
    inconsistencia_encontrada = False

    for archivo in os.listdir(ruta_zips):

        match = re.match(patron, archivo)

        if match:

            anio_actual = match.group(1)
            mes_actual = match.group(2)
            tipo = match.group(3)
            version = match.group(4)

            if anio_esperado is None:
                anio_esperado = anio_actual
                mes_esperado = mes_actual

            else:
                if anio_actual != anio_esperado or mes_actual != mes_esperado:
                    print(f"Error: Inconsistencia en fechas. Esperado: {anio_esperado}-{mes_esperado}, encontrado: {anio_actual}-{mes_actual} en archivo {archivo}")
                    inconsistencia_encontrada = True
                    break
            
            
            zip_path = os.path.join(ruta_zips, archivo)
            destino = tipo
            
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(destino)
                print(f"Descomprimido {archivo} en {destino}/")
                archivos_procesados += 1
            except zipfile.BadZipFile:
                print(f"Error: {archivo} no es un archivo ZIP válido")
            except Exception as e:
                print(f"Error al descomprimir {archivo}: {str(e)}")
    
    if inconsistencia_encontrada:

        print("Proceso terminado debido a inconsistencias en las fechas, solo colocar archivos con el mismo año y fecha.")

        for carpeta in carpetas:

            if os.path.exists(carpeta):

                shutil.rmtree(carpeta)
            
        return None
    
    if archivos_procesados == 0:

        print("No se encontraron archivos ZIP válidos para procesar")
        for carpeta in carpetas:

            if os.path.exists(carpeta):

                shutil.rmtree(carpeta)
        return None
    
    return (anio_esperado, mes_esperado, version)

def BuscarRuta(directorio: str, patron: str, descripcion: str):

    if not os.path.isdir(directorio):
        print(f"Error: El directorio no existe-> {directorio}")
        return None
    
    try:

        for archivo in os.listdir(directorio):

            if re.match(patron, archivo, re.IGNORECASE):

                return os.path.join(directorio, archivo)
        
    except PermissionError:

        print(f"Error: Sin permiso para acceder a la ruta-> {directorio}")
        return None

    print(f"Error: No se encontró {descripcion} en {directorio}.")
    return None

def Escribir_Archivo(ruta_archivo, tabla_parquet):

    pq.write_table(tabla_parquet, ruta_archivo ,compression='snappy')

def PSUF (anio, mes, version):

    hojas_psuf=['CERRO_DOMINADOR_CSP','HIDROELECTRICA SAN ANDRES','HIDROELECTRICA LOS CORRALE','EL_AGRIO','LOS_PADRES','ROBLERIA','DOS_VALLES','PALACIOS','PIEDRAS_NEGRAS','PUNTA DEL VIENTO','LIKANA','SGA']
    orden_columnas = ["Año", "Mes", "Versión", "Descripción", "Barra", "Tipo Clave", "Medida (MW)", "Precio Nudo ($/KW/mes)", "Pago PSUF ($)", "Empresa"]

    ruta_potencia = "Potencia/"
    patron_detalle_empresa = r'.*?empresa'
    patron_archivo = r'.*?(empresas|EMPRESAS|Empresas)\.(xlsx|xlsm)?'

    esquema = pa.schema([
        ("Año", pa.int16()),
        ("Mes", pa.int16()),
        ("Versión", pa.string()),
        ("Descripción", pa.string()),
        ("Barra", pa.string()),
        ("Tipo Clave", pa.string()),
        ("Medida (MW)", pa.float64()),
        ("Precio Nudo ($/KW/mes)", pa.float64()),
        ("Pago PSUF ($)", pa.float64()),
        ("Empresa", pa.string())
    ])


    subdirectorio_empresa = BuscarRuta(ruta_potencia, patron_detalle_empresa, "Detalle por empresa")
    if not subdirectorio_empresa:
        return 1

    ruta_excel = BuscarRuta(subdirectorio_empresa, patron_archivo, "BDef EMPRESAS.xlsx")

    if not ruta_excel:
        return 1

    xlsx = pd.ExcelFile(ruta_excel)
    
    DataFrames_Dic = []
    for pagina in hojas_psuf:

        try:

            df_temp = xlsx.parse(sheet_name=pagina, header=5)
            df_temp['Empresa'] = pagina
            DataFrames_Dic.append(df_temp)
            
        except ValueError:

            print("Error al leer la hoja: ", pagina)

    if len(DataFrames_Dic) == 0:

        print("Error: No se cargó data a PSUF.")
        return 1

    df = pd.concat(DataFrames_Dic, ignore_index=True)

    df['Año'] = anio
    df['Mes'] = mes
    df['Versión'] = version
    df = df[orden_columnas]

    tabla_P = pa.Table.from_pandas(df,schema=esquema, preserve_index= False)

    nombre_archivo = "PSUF.parquet"
    ruta_destino = os.path.join(ruta_destino_archivos_parquet, nombre_archivo)

    Escribir_Archivo(tabla_parquet=tabla_P, ruta_archivo=ruta_destino)

    return 0

def PEST(anio, mes, version):

    ruta_energia = "Energia/"
    
    patron_resultados = r'.*?Resultados.*' # Revisar que sea dir
    patron_energia_balance = r'^Balance.*\.xlsx$'
    hoja_excel = "Cuadro de Energía"

    orden_columnas = [
        "Año",
        "Mes",
        "Versión",
        "Razon_Social",
        "Empresa",
        "RUT",
        "Inyecciones y Retiros de Energía",
        "Asignación IT Nacional  Antigua",
        "Asignación IT Dedicada",
        "Asignación IT Nacional  Nueva",
        "Asignación IT Dedicada uso Regulado",
        "Asignación IT Zonal",
        "Asignación Saldo SAE",
        "Ingresos Adicionales Precio Nudo/Estabilizado",
        "Compensación precio nudo/estabilizado",
        "Contratos Compraventas Físicos",
        "Sobrecosto PD",
        "Sobrecostos Energía",
        "Total CLP"
    ]

    esquema = pa.schema([
        ("Año", pa.int16()),
        ("Mes", pa.int16()),
        ("Versión", pa.string()),
        ("Razon_Social", pa.string()),
        ("Empresa", pa.string()),
        ("RUT", pa.string()),
        ("Inyecciones y Retiros de Energía", pa.float64()),
        ("Asignación IT Nacional  Antigua", pa.float64()),
        ("Asignación IT Dedicada", pa.float64()),
        ("Asignación IT Nacional  Nueva", pa.float64()),
        ("Asignación IT Dedicada uso Regulado", pa.float64()),
        ("Asignación IT Zonal", pa.float64()),
        ("Asignación Saldo SAE", pa.float64()),
        ("Ingresos Adicionales Precio Nudo/Estabilizado", pa.float64()),
        ("Compensación precio nudo/estabilizado", pa.float64()),
        ("Contratos Compraventas Físicos", pa.float64()),
        ("Sobrecosto PD", pa.float64()),
        ("Sobrecostos Energía", pa.float64()),
        ("Total CLP", pa.float64())
    ])


    ruta_resultados = BuscarRuta(ruta_energia, patron_resultados, "Resultados")
    if not ruta_resultados:
        return 1
    
    ruta_final = BuscarRuta(ruta_resultados, patron_energia_balance, "Balance")
    if not ruta_final:
        return 1

    try:

        df = pd.read_excel(ruta_final, sheet_name=hoja_excel, header=2, usecols="A:P")

    except ValueError:

        print("Error al abrir el excel y la hoja.")
        return 1
    
    df['Año'] = anio
    df['Mes'] = mes
    df['Versión'] = version
    df = df[orden_columnas]

    tabla_P = pa.Table.from_pandas(df,schema=esquema, preserve_index= False)

    nombre_archivo = "PEST.parquet"
    ruta_destino = os.path.join(ruta_destino_archivos_parquet, nombre_archivo)

    Escribir_Archivo(tabla_parquet=tabla_P, ruta_archivo=ruta_destino)

    return 0

def Obtener_Rutas_SSCC():

    ruta_sscc = "SSCC/"
    ruta_energia = "Energia/"

    patron_resultados_energia = r'.*?Resultados.*' # Revisar que sea dir
    patron_balance_energia = r'.*?Balance.*' # Revisar que sea dir
    patron_sobrecostos_energia = r'.*?Sobrecostos.*' # Revisar que sea dir
    
    patron_archivo_pago_sobrecostos = r'Pago.*Sobrecostos.*\.xlsx$' # Este está en energia

    patron_archivo_remuneracion = r'\d*_REMUNERACI.N.*?SC.*\.xlsx$' # Estos dos están en SSCC
    patron_archivo_pagos_retiros = r'\d*_Pagos.*?Retiros.*\.xlsx$'

    lista_rutas = []

    ruta_resultados = BuscarRuta(ruta_energia, patron_resultados_energia, "Resultados")
    if not ruta_resultados:
        return None
    
    ruta_energia_balance = BuscarRuta(ruta_resultados, patron_balance_energia, "Balance de Energia")
    if not ruta_energia_balance:
        return 1
    
    ruta_energia_balance_sobrecosto = BuscarRuta(ruta_energia_balance, patron_sobrecostos_energia, "Sobrecostos")
    if not ruta_energia_balance_sobrecosto:
        return 1
    
    ruta_final = BuscarRuta(ruta_energia_balance_sobrecosto, patron_archivo_pago_sobrecostos, "Pago Sobrecostos")
    if not ruta_final:
        return 1

    
    lista_rutas.append(ruta_final)


    ruta_temp = None

    ruta_temp = BuscarRuta(ruta_sscc, patron_archivo_remuneracion, "Remuneracion Sc")
    if not ruta_temp:
        return 1
    
    lista_rutas.append(ruta_temp)

    ruta_temp = None

    ruta_temp = BuscarRuta(ruta_sscc, patron_archivo_pagos_retiros, "Pagos Retiros")
    if not ruta_temp:
        return 1
    
    lista_rutas.append(ruta_temp)


    if len(lista_rutas) == 3:

        return lista_rutas

    else:
        print(f"Error en buscar los archivos en SSCC/: {ruta_sscc}")
        return None

def SSCC(anio, mes, version):

    orden_columnas = ["Año", "Mes", "Versión", "Concepto", "Tipo_sobrecosto", "clave", "Retiro", "Tipo", "Barra", "Suministrador", "Suma de Pago"]
    nombre_hoja = "PAGO_RETIRO"
    esquema = pa.schema([
        ("Año", pa.int16()),
        ("Mes", pa.int16()),
        ("Versión", pa.string()),
        ("Concepto", pa.string()),
        ("Tipo_sobrecosto", pa.string()),
        ("clave", pa.string()),
        ("Retiro", pa.string()),
        ("Tipo", pa.string()),
        ("Barra", pa.string()),
        ("Suministrador", pa.string()),
        ("Suma de Pago", pa.float64())
    ])

    lista_rutas = Obtener_Rutas_SSCC()
    DataFrames_dic = []

    for ruta in lista_rutas:
        try:

            df_temp = pd.read_excel(ruta, sheet_name=nombre_hoja)
            DataFrames_dic.append(df_temp)

        except ValueError as e:

            print("Error al cargar una ruta: ", ruta)
            print(f"Detalle del error: {e}")

    if len(DataFrames_dic) == 0:

        print("No se cargó data.")
        return 1
    
    df = pd.concat(DataFrames_dic, ignore_index=True)

    df['Año'] = anio
    df['Mes'] = mes
    df['Versión'] = version
    df = df[orden_columnas]

    tabla_P = pa.Table.from_pandas(df,schema=esquema, preserve_index= False)

    nombre_archivo = "SSCC.parquet"
    ruta_destino = os.path.join(ruta_destino_archivos_parquet, nombre_archivo)

    Escribir_Archivo(tabla_parquet=tabla_P, ruta_archivo=ruta_destino)

    return 0

def CVF(anio, mes, version):

    ruta_energia = "Energia/"
    hoja_excel = "Balance Valorizado"
    patron_energia = r'.*?Resultados.*' # Revisar que sea dir
    patron_energia_balance = r'^Balance.*\.xlsx$'
    orden_columas = ["Id_Contrato", "Clave", "Empresa", "fisico_kwh", "monetario"]

    esquema = pa.schema([
        ('Id_Contrato', pa.int64()),
        ('Clave', pa.string()),
        ('Empresa', pa.string()),
        ('COMPRA', pa.float64()),
        ('VENTA', pa.float64()),
        ('COMPRA_1', pa.float64()),
        ('VENTA_1', pa.float64()),
        ("Año", pa.int16()),
        ("Mes", pa.int16()),
        ("Versión", pa.string())
    ])

    ruta_resultados = BuscarRuta(ruta_energia, patron_energia, "Resultados")
    if not ruta_resultados:
        return 1
    
    ruta_final = BuscarRuta(ruta_resultados, patron_energia_balance, "Balance")
    if not ruta_final:
        return 1
    
    
    try:

        df = pd.read_excel(ruta_final, sheet_name=hoja_excel)

    except ValueError:

        print("Error al abrir el excel y la hoja.")
        return 1

    #print(df.columns)

    if 'Zona' in df.columns:

        df = df[df['Zona'] == "Compraventa"]
        df.drop(columns=["calificacion_linea", "numero_linea", "RUT", "barra", "nivel_tension", "Zona", "tipo_medidor", "nombre_empresa"], inplace=True)

    elif 'tipo_medidor' in df.columns:
    
        df = df[(df['tipo_medidor'] == 'C_FIS') | (df['tipo_medidor'] == 'C_FIN')]
        df.drop(columns=["calificacion_linea", "numero_linea", "RUT", "barra", "nivel_tension", "tipo_medidor", "nombre_empresa"], inplace=True)
    
    else:

        print("La columna Zona y tipo_medidor no están presentes en la tabla, no se puede filtrar compraventa sin ninguno de estos dos. En realidad, tal vez si se puede, se deja un comentario después de este print para las indicaciones.")

        # Si Zona y tipo_medidor no están presentes, puede existir TAL VEZ otras dos formas de filtrar la compraventa:

        # Por nombre_medidor: Todas las compraventas (Esto hay que revisarlo igualmente) tiene de nombre_medidor un valor numérico. Se puede filtrar con esta condición.

        # Por clave_medidor: Al parecer todas las compraventas comienzan con 'V_' o 'C_'. Si se puede verificar esta condición, habria que hacer una expresión regular para filtrar:
        # r'^C_.*' por ejemplo

        return None

    df.rename(columns={'nombre_medidor': 'Id_Contrato', 'nombre_corto_empresa': 'Empresa', 'clave_medidor': 'Clave'}, inplace=True)
    
    df['Id_Contrato'] = df['Id_Contrato'].astype(int)

    df = df[orden_columas]

    df['COMPRA'] = 0.0
    df['VENTA'] = 0.0
    df['COMPRA_1'] = 0.0
    df['VENTA_1'] = 0.0

    mascara_positiva = df['fisico_kwh'] > 0

    df.loc[mascara_positiva, 'COMPRA'] = df.loc[mascara_positiva, 'fisico_kwh']
    df.loc[mascara_positiva, 'COMPRA_1'] = df.loc[mascara_positiva, 'monetario']

    mascara_negativa = df['fisico_kwh'] < 0

    df.loc[mascara_negativa, 'VENTA'] = -df.loc[mascara_negativa, 'fisico_kwh']
    df.loc[mascara_negativa, 'VENTA_1'] = -df.loc[mascara_negativa, 'monetario']

    df.drop(columns=['fisico_kwh', 'monetario'], inplace=True)

    df['Año'] = anio
    df['Mes'] = mes
    df['Versión'] = version

    tabla_P = pa.Table.from_pandas(df,schema=esquema, preserve_index= False)

    nombre_archivo = "CVF.parquet"
    ruta_destino = os.path.join(ruta_destino_archivos_parquet, nombre_archivo)

    Escribir_Archivo(tabla_parquet=tabla_P, ruta_archivo=ruta_destino)

    return 0

def Asignacion_Dx(anio, mes, version):

    ruta_energia = 'Energia/'
    patron_antecendentes = r'.*?Antecedentes.*'
    patron_asignacion = r'.*?(Dx|dx|DX|dX)'
    patron_archivo_A_DX = r'(Asignacion|asignacion).*?\.csv'

    patron_energia = r'.*?Resultados.*' # Revisar que sea dir
    patron_energia_balance = r'^Balance.*\.xlsx$'

    hoja_balance = "Balance Valorizado"

    orden_columnas = ['Año', 'Mes', 'Versión', 'Bloque Regulado', 'Suministrador', 'kWh Punto Suministro', 'Porcentaje', 'Fisico KWh', 'Monetario']

    esquema = pa.schema([
        ("Año", pa.int16()),
        ("Mes", pa.int16()),
        ("Versión", pa.string()),
        ("Bloque Regulado", pa.string()),
        ("Suministrador", pa.string()),
        ("kWh Punto Suministro", pa.float64()),
        ("Porcentaje", pa.float64()),
        ("Fisico KWh", pa.float64()),
        ("Monetario", pa.float64())
    ])

    #Obtener ruta del csv de Asignacion Dx
    ruta_antecendes = BuscarRuta(ruta_energia, patron_antecendentes, "Antecendentes de cálculo")
    if not ruta_antecendes:
        return 1
    
    ruta_asignacion = BuscarRuta(ruta_antecendes, patron_asignacion, "Asignacion Dx")
    if not ruta_asignacion:
        return 1
    
    ruta_archivo_A_dx = BuscarRuta(ruta_asignacion, patron_archivo_A_DX, "Asignacion Dx CSV")
    if not ruta_archivo_A_dx:
        return 1

    #Obtener ruta del excel de Balance
    ruta_resultados = BuscarRuta(ruta_energia, patron_energia, "Resultados")
    if not ruta_resultados:
        return 1
    
    ruta_archivo_balance = BuscarRuta(ruta_resultados, patron_energia_balance, "Balance")
    if not ruta_archivo_balance:
        return 1

    #Abrir el archivo de balance
    try:

        df_balance = pd.read_excel(ruta_archivo_balance, sheet_name=hoja_balance)

    except ValueError:

        print("Error abriendo el archivo excel de Balance.")
        return 1

    #Abrir el archivo de asignacion
    try:

        df_asignacion = pd.read_csv(ruta_archivo_A_dx, delimiter=';')
    
    except ValueError:

        print("Error abriendo el archivo csv de Asignacion Dx")
        return 1


    df_balance = df_balance[df_balance['tipo_medidor'] == 'R']

    if "Zona" in df_balance.columns:

        try:

            df_balance.drop(columns=['RUT', 'calificacion_linea', 'numero_linea', 'nivel_tension', 'barra', 'clave_medidor', 'tipo_medidor', 'Zona'], inplace=True)
        
        except KeyError as e:

            print(f"Error: Algunas columnas que se quieren borrar de Balance->Balance Valorizado no están presentes en el archivo. Revisar manualmente. \n{e}")
            return 1
    
    else:

        try:

            df_balance.drop(columns=['RUT', 'calificacion_linea', 'numero_linea', 'nivel_tension', 'barra', 'clave_medidor', 'tipo_medidor'], inplace=True)

        except KeyError as e:

            print(f"Error: Algunas columnas que se quieren borrar de Balance->Balance Valorizado no están presentes en el archivo. Revisar manualmente. \n{e}")
            return 1

    df_balance_resumido = df_balance.groupby("nombre_empresa").agg(
        fisico_total=("fisico_kwh", "sum"),
        monetario_total=("monetario", "sum")
    ).reset_index()


    try:
        df_asignacion.drop(columns=['RUT Suministrador', 'CODIGO_CNE', 'id Contrato', 'peridiocidad', 'año', 'fecha inicio', 'fecha fin', 'clasificación', 'versión'], inplace=True)
    
    except KeyError as e:

        print(f"Error: Algunas columnas que se quieren borrar de AsignacionDx.csv no están presentes en el archivo. Revisar manualmente. \n {e}")
    
    df_asignacion = df_asignacion.groupby(["grupbalance", "Suministrador"],as_index=False ).agg(
        {"Energía_kwh": "sum"}
    )
        
    df_asignacion['Energia_total'] = df_asignacion.groupby("grupbalance")['Energía_kwh'].transform("sum")
    df_asignacion['Porcentaje'] = df_asignacion['Energía_kwh'] / df_asignacion['Energia_total']


    df_final = pd.merge(
        df_asignacion,
        df_balance_resumido,
        left_on = "grupbalance",
        right_on = "nombre_empresa",
        how="inner"
    )

    df_final['Fisico KWh'] = df_final['fisico_total'] * df_final['Porcentaje']
    df_final['Monetario'] = df_final['monetario_total'] * df_final['Porcentaje']

    df_final.drop(columns=['Energia_total', 'nombre_empresa', 'fisico_total', 'monetario_total'], inplace=True)
    df_final.rename(columns={'grupbalance': 'Bloque Regulado', 'Energía_kwh': 'kWh Punto Suministro'}, inplace=True)


    df_final['Año'] = anio
    df_final['Mes'] = mes
    df_final['Versión'] = version

    df_final = df_final[orden_columnas]

    tabla_P = pa.Table.from_pandas(df_final,schema=esquema, preserve_index= False)

    nombre_archivo = "AsignacionDx.parquet"
    ruta_destino = os.path.join(ruta_destino_archivos_parquet, nombre_archivo)

    Escribir_Archivo(tabla_parquet=tabla_P, ruta_archivo=ruta_destino)

    return 0


if __name__ == "__main__":


    initialize_storage_account_sas("grupocerrounodatalake","sp=racwdlmeop&st=2024-11-05T20:44:46Z&se=2100-11-06T04:44:46Z&sv=2022-11-02&sr=c&sig=ASobPbtE7jPqsvnz2E243hwatOeuqoi4OGs6OxJNSME%3D")

    valores = descomprimir_Zip()

    if valores == None:

        lista_carpetas = ["Energia", "Potencia", "SSCC", "archivos_parquet"]
        for carpeta in lista_carpetas:
            if os.path.exists(carpeta):
                shutil.rmtree(carpeta)
        exit()

    anio = int(valores[0])
    mes = int(valores[1])
    version = str(valores[2])

    valor_psuf = PSUF(anio, mes, version)
    if valor_psuf:
        print("Error: La función PSUF presenta un error.")

    valor_pest = PEST(anio, mes, version)
    if valor_pest:
        print("Error: La función PEST presenta un error.")

    valor_sscc = SSCC(anio, mes, version)
    if valor_sscc:
        print("Error: La función SSCC presenta un error.")

    valor_cvf = CVF(anio, mes, version)
    if valor_cvf:
        print("Error: La función CVF presenta un error.")

    valor_asignacion = Asignacion_Dx(anio, mes, version)
    if valor_asignacion:
        print("Error: La función Asignacion DX presenta un error.")

    nombre_archivo_datalake = f"{anio}-{mes}.parquet"

    carpeta_archivos_parquet = os.path.join(os.path.dirname(__file__), ruta_destino_archivos_parquet)

    for archivo_parquet in os.listdir(carpeta_archivos_parquet):

        ruta_completa = os.path.join(carpeta_archivos_parquet, archivo_parquet)

        if os.path.isfile(ruta_completa):

            clave_dic = os.path.splitext(archivo_parquet)[0]

            cargar_archivo(
                ruta= dic_rutas_datalake[clave_dic],
                local_file_path = ruta_completa,
                nombre_archivo= nombre_archivo_datalake
            )



