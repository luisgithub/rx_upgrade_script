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
            datetime_param = datetime.date(2024, 12, 31)  # Example datetime parameter
            cursor.execute(query, (datetime_param, page_size, offset))  # Pass datetime_param as a tuple
            columns = [desc[0] for desc in cursor.description] # Get column names
            results = [dict(zip(columns, row)) for row in cursor.fetchall()] # Map rows to dictionaries
            cursor.close()
            pool_manager.put_connection(connection)  # Release the connection
            return results
        else:
            return []
    except psycopg2.Error as e:
        logger.error(f"Error while fetching rx data: {e}")
    # finally:
    #     pool_manager.close_pool() #close the pool when done.

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

def get_id_producto_by_description_and_clinic(producto_descripcion, clinica):
    try:
        connection = get_connection()
        if(connection):
            cursor = connection.cursor()
            logger.info(f"Parametros: {clinica}")
            query = "SELECT id FROM producto p WHERE p.clinica = %s AND p.descripcion = %s "
            params = (int(clinica), producto_descripcion)
            cursor.execute(query, params)
            result = cursor.fetchone()
            if result is None:
                return None
            if cursor.fetchone() is not None:  # Check if there's another row
                raise Exception("Query returned more than one result.  Expected single result.")
            return result[0]
    except psycopg2.Error as e:  # Handle database errors
        logger.error(f"Error while fetching product data : {e}")
        return None  


def close():
    pool_manager.close_pool() #close the pool when done.
