from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

LIMITE_REGISTROS = 1000
MAGNITUD_MINIMA = 4.0
OUTPUT_PATH = './output'
SISMOS_DATA_PATH = './output/sismos_data.csv'

# Configuración por defecto del DAG
default_args = {
    'owner': 'Sebastian Arguello',
    'depends_on_past': False,
    'start_date': datetime.today() - timedelta(days=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
dag = DAG(
    'earthquake_data_pipeline',
    default_args=default_args,
    description='Pipeline para extraer datos de sismos de USGS API',
    schedule=None,  
    catchup=False,
    tags=['earthquakes', 'usgs', 'geoscience', 'sismos'],
)


def create_output_directory(**context):
    """
    Crear directorio output si no existe
    """
    output_dir = Path(OUTPUT_PATH)
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Directorio de salida creado/verificado: {output_dir}")
    return str(output_dir)


def fetch_earthquake_data(**context):
    """
    Extraer datos de terremotos de la API de USGS
    Parámetros: sismos > 4.0 magnitud del último año, límite 1000
    """
    # Calcular fechas (último año)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    # Parámetros de la consulta
    base_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        'format': 'geojson',
        'starttime': start_date.strftime('%Y-%m-%d'),
        'endtime': end_date.strftime('%Y-%m-%d'),
        'minmagnitude': MAGNITUD_MINIMA,
        'limit': LIMITE_REGISTROS,
        'orderby': 'time'
    }
    
    print(f"Consultando sismos desde: {start_date.strftime('%Y-%m-%d')}")
    print(f"Hasta: {end_date.strftime('%Y-%m-%d')}")
    print(f"Parámetros: {params}")
    
    try:
        # Realizar consulta a la API
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        
        # Parsear respuesta JSON
        data = response.json()
        
        # Información de la respuesta
        total_earthquakes = len(data.get('features', []))
        print(f"Total de sismos obtenidos: {total_earthquakes}")
        
        # Guardar datos raw en XCom para siguiente tarea
        return {
            'raw_data': data,
            'total_count': total_earthquakes,
            'fetch_timestamp': datetime.now().isoformat()
        }
        
    except requests.exceptions.RequestException as e:
        print(f"Error al consultar la API de USGS: {e}")
        raise
    except json.JSONDecodeError as e:
        print(f"Error al parsear JSON de respuesta: {e}")
        raise


def process_earthquake_data(**context):
    """
    Procesar los datos GeoJSON y convertirlos a formato tabular
    """
    # Obtener datos de la tarea anterior
    ti = context['ti']
    api_response = ti.xcom_pull(task_ids='fetch_earthquake_data')
    
    if not api_response or 'raw_data' not in api_response:
        raise ValueError("No se encontraron datos de la tarea anterior")
    
    raw_data = api_response['raw_data']
    features = raw_data.get('features', [])
    
    if not features:
        print("No se encontraron eventos sísmicos")
        return None
    
    # Lista para almacenar datos procesados
    processed_data = []
    
    for feature in features:
        try:
            # Extraer propiedades del evento
            properties = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            coordinates = geometry.get('coordinates', [None, None, None])
            
            # Procesar timestamp
            time_ms = properties.get('time')
            if time_ms:
                earthquake_time = datetime.fromtimestamp(time_ms / 1000.0)
            else:
                earthquake_time = None
            
            # Crear registro procesado
            record = {
                'id': feature.get('id'),
                'timestamp': earthquake_time,
                'magnitude': properties.get('mag'),
                'magnitude_type': properties.get('magType'),
                'place': properties.get('place'),
                'longitude': coordinates[0],
                'latitude': coordinates[1],
                'depth_km': coordinates[2],
                'alert_level': properties.get('alert'),
                'tsunami_risk': properties.get('tsunami', 0),
                'significance': properties.get('sig'),
                'event_type': properties.get('type'),
                'network': properties.get('net'),
                'updated': datetime.fromtimestamp(properties.get('updated', 0) / 1000.0) if properties.get('updated') else None,
                'detail_url': properties.get('detail')
            }
            
            processed_data.append(record)
            
        except Exception as e:
            print(f"Error procesando evento {feature.get('id', 'unknown')}: {e}")
            continue
    
    print(f"Eventos procesados exitosamente: {len(processed_data)}")
    
    # Retornar datos procesados para siguiente tarea
    return processed_data


def create_csv_file(**context):
    """
    Crear archivo CSV con los datos procesados
    """
    # Obtener datos procesados
    ti = context['ti']
    processed_data = ti.xcom_pull(task_ids='process_earthquake_data')
    
    if not processed_data:
        raise ValueError("No se encontraron datos procesados")
    
    # Convertir a DataFrame
    df = pd.DataFrame(processed_data)
    
    # Información del dataset
    print(f"Dimensiones del dataset: {df.shape}")
    print(f"Columnas: {list(df.columns)}")
    
    # Estadísticas básicas
    if not df.empty:
        print(f"Rango de magnitudes: {df['magnitude'].min():.1f} - {df['magnitude'].max():.1f}")
        print(f"Rango de fechas: {df['timestamp'].min()} - {df['timestamp'].max()}")
        print(f"Países/regiones principales:")
        place_counts = df['place'].str.extract(r'([A-Za-z\s]+)$')[0].value_counts().head(5)
        print(place_counts)
    
    # Definir ruta del archivo
    output_dir = Path(OUTPUT_PATH)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'Sismos_data_{timestamp}.csv'
    file_path = output_dir / filename
    
    # Guardar CSV
    df.to_csv(file_path, index=False, encoding='utf-8')
    
    print(f"Archivo CSV creado: {file_path}")
    print(f"Tamaño del archivo: {file_path.stat().st_size / 1024:.2f} KB")
    
    



# Definición de las tareas del DAG
# =================================

# Tarea 1: Crear directorio de salida
create_output_dir = PythonOperator(
    task_id='create_output_directory',
    python_callable=create_output_directory,
    dag=dag,
)

# Tarea 2: Extraer datos de la API de USGS
fetch_data = PythonOperator(
    task_id='fetch_earthquake_data',
    python_callable=fetch_earthquake_data,
    dag=dag,
)

# Tarea 3: Procesar datos GeoJSON
process_data = PythonOperator(
    task_id='process_earthquake_data',
    python_callable=process_earthquake_data,
    dag=dag,
)

# Tarea 4: Crear archivo CSV
create_csv = PythonOperator(
    task_id='create_csv_file',
    python_callable=create_csv_file,
    dag=dag,
)

# Definir dependencias del pipeline
# =================================
create_output_dir >> fetch_data >> process_data >> create_csv