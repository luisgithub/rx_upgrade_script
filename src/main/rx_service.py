import psycopg2
from src.main.db_connection import DatabaseConnection
from src.main.utils.logger_config import setup_logger
import datetime

logger = setup_logger("erposm - ")
pool_manager = DatabaseConnection()

def get_connection():
    return pool_manager.get_connection()

def fetch_rx_records(page_size, offset):
    
    try:
        # Get a connection from the pool
        record_list = []
        connection = get_connection()
        if connection:
            cursor = connection.cursor()    
            query = """SELECT id, lente_descripcion, tipo_lente_descripcion, clinica FROM "rx" WHERE fecha > %s ORDER BY id LIMIT %s OFFSET %s;"""
            datetime_param = datetime.date(2018, 12, 31)  # Example datetime parameter
            cursor.execute(query, (datetime_param, page_size, offset))  # Pass datetime_param as a tuple
            columns = [desc[0] for desc in cursor.description] # Get column names
            results = [dict(zip(columns, row)) for row in cursor.fetchall()] # Map rows to dictionaries

            return results
        else:
            return []
    except psycopg2.Error as e:
        logger.error(f"Error while fetching rx data: {e}")
    finally:
        cursor.close()
        pool_manager.put_connection(connection)  # Release the connection

def get_id_tipo_producto_by_description(tipo_desc):
    Dict = {
        'Bifocal':'1',
        'Lente de Conctacto':'2',
        'Progresivo':'3',
        'Solo Cerca':'4',
        'Solo Lejos':'5',
        'Vision Sencilla':'6',
    }
    return Dict[tipo_desc]
def get_id_color_by_descripcion(desc):
    Dict = {
        'GRIS 1':1,
        'GRIS 2':2, 
        'GRIS 3':3,	
        'CAFE 1':4,
        'CAFE 2':5,	
        'CAFE 3':6,	
        'AMARILLO':7,	
        'AZUL':8,	
        'MARINO':9,	
        'VERDE':10,	
        'SAHARA':11,	
        'ROSA':12	
    }

def get_id_producto_by_description_and_clinic(producto_descripcion, clinica):
    try:
        connection = get_connection()
        if(connection):
            cursor = connection.cursor()
            query = "SELECT id FROM producto p WHERE p.clinica = %s AND p.descripcion = %s "
            params = (int(clinica), producto_descripcion)
            cursor.execute(query, params)
            result = cursor.fetchone()
            if result is None:
                return None
            if len(result) > 1:
                logger.info(f"Producto duplicado: {producto_descripcion}")
                return None
            # if cursor.fetchone() is not None:  # Check if there's another row
            #     raise Exception("Query returned more than one result.  Expected single result.")
            return result[0]
    except psycopg2.Error as e:  # Handle database errors
        logger.error(f"Error while fetching product data : {e}")
        return None  
    finally:
        cursor.close()
        pool_manager.put_connection(connection)


def execute_update_query(sql):
    try:
        connection = get_connection()
        if(connection):
            cursor = connection.cursor()
            cursor.execute(sql)
            connection.commit()
            # logger.info(sql)
    except psycopg2.Error as e:
        logger.error(f"Error al ejecutar SQL: {sql}")
    finally:
        cursor.close()
        pool_manager.put_connection(connection)


def fetch_rx_records_color(page_size, offset):
    try:
        connection = get_connection()
        if connection:
            cursor = connection.cursor()
            query = '''SELECT id, color FROM rx ORDER BY id LIMIT %s OFFSET %s;'''
            cursor.execute(query, (page_size, offset))
            cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return results
        else:
            return []
    except psycopg2.Error as e:
        logger.error(f"Error al ejecutar SQL: {query}")

def close():
    pool_manager.close_pool() #close the pool when done.
