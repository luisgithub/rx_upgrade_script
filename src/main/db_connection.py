
import psycopg2
from psycopg2 import pool
from src.main.utils.environment_config import environment_configs
from src.main.utils.logger_config import setup_logger



# Minimum and maximum pool size
MIN_CONNECTIONS = 2
MAX_CONNECTIONS = 5

logger = setup_logger("erposm -")

class DatabaseConnection:
    
    _instance = None  # Singleton instance
    
    def __init__(self):
        self.pool = None
        self._create_pool()

    def _create_pool(self):
        config = environment_configs.get("env")
        if not config:
            logger.error("No se encontro la configuracion de la base de datos..")

        try:
            self.pool = pool.SimpleConnectionPool(
                MIN_CONNECTIONS,
                MAX_CONNECTIONS,
                dbname = config['database'],
                user = config['user'],
                password = config['password'],
                host = config['host'],
                port = config['port'],
            )
            logger.info(f"PostgreSQL connection pool created successfully..")
        except psycopg2.Error as e:
            logger.error(f"Error creating connection pool: {e}")

    @classmethod
    def get_instance(cls):
        """Returns the singleton instance of the DatabaseConnection."""
        if cls._instance is None:
            cls._instance = DatabaseConnection()
        return cls._instance

    def get_connection(self):
        """Gets a connection from the pool."""
        if self.pool:
            return self.pool.getconn()
        return None

    def put_connection(self, conn):
        """Returns a connection to the pool."""
        if self.pool and conn:
            self.pool.putconn(conn)

    def close_pool(self):
        """Closes the connection pool."""
        if self.pool:
            self.pool.closeall()
            print("Database connection pool closed.")
            self.pool = None  # Important: Reset the pool after closing