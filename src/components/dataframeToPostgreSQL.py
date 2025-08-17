import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import Optional, List, Dict, Any
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataFrameToPostgreSQL:
    def __init__(
            self, 
            host='localhost', 
            database=None, 
            user=None, 
            password=None, 
            port=5433):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.engine = None
        self.connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    def create_engine(self):
        """Create SQLAlchemy engine"""
        try:
            self.engine = create_engine(
                self.connection_string,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            logger.info("SQLAlchemy engine created successfully")
            return self.engine
        except Exception as e:
            logger.error(f"Error creating engine: {e}")
            raise
    
    def test_connection(self):
        """Test database connection"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                logger.info("Database connection test successful")
                return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def pandas_to_sql(
            self,
            df, 
            schema_name,
            table_name):
            """Creating an SQL table from Pandas DataFrame, with schema specificity"""
                # Insert the DataFrame into the specified schema and table
            try:
                df.to_sql(name=table_name, con=self.engine, schema=schema_name, if_exists='append', index=False)
                print(f"DataFrame successfully inserted into {schema_name}.{table_name}")
            except Exception as e:
                print(f"Error inserting DataFrame: {e}")
        