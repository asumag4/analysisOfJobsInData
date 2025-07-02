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
    def __init__(self, host='localhost', database='your_database', 
                 user='your_username', password='your_password', port=5432):
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
    
    def pandas_to_sql(self, df: pd.DataFrame,
                     table_name: str,
                     schema_name: str = None,
                     project_name: str = None,
                     if_exists: str = 'append',
                     index: bool = False,
                     chunksize: Optional[int] = 10000) -> bool:
        """
        Enhanced pandas to_sql method with schema support
        Most robust and widely accepted approach for DataFrame insertion
        
        Args:
            df: DataFrame to insert
            table_name: Name of the target table
            schema_name: Explicit schema name (e.g., 'shared', 'logs', 'project_finance')
            project_name: Project name (will be converted to schema format if schema_name not provided)
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            index: Whether to write DataFrame index as a column
            chunksize: Number of rows to write at a time
        
        Returns:
            bool: True if successful, False otherwise
        """
        import time
        
        try:
            start_time = time.time()
            
            # Determine schema - priority: explicit schema_name > project_name > current_project
            if schema_name:
                target_schema = schema_name
            elif project_name:
                target_schema = self.get_project_schema(project_name)
            elif self.current_project:
                target_schema = self.get_project_schema(self.current_project)
            else:
                # Default to public schema if nothing specified
                target_schema = 'public'
                logger.warning("No schema specified, using 'public' schema")
            
            full_table_name = f"{target_schema}.{table_name}"
            
            # Log the operation
            log_project = project_name or self.current_project or 'unknown'
            self.log_message(
                log_project,
                'INFO',
                f"Inserting {len(df)} rows into {full_table_name} using pandas.to_sql()"
            )
            
            # Insert data using pandas to_sql with schema specification
            df.to_sql(
                name=table_name,
                con=self.engine,
                schema=target_schema,
                if_exists=if_exists,  # 'fail', 'replace', 'append'
                index=index,
                chunksize=chunksize,
                method='multi'  # Use multi-row INSERT for better performance
            )
            
            end_time = time.time()
            success_msg = f"Successfully inserted {len(df)} rows into {full_table_name} in {end_time - start_time:.2f} seconds"
            logger.info(success_msg)
            
            # Log successful completion
            self.log_message(
                log_project,
                'INFO',
                success_msg
            )
            
            return True
            
        except Exception as e:
            error_msg = f"Error inserting DataFrame into {table_name} (schema: {target_schema if 'target_schema' in locals() else 'unknown'}): {e}"
            logger.error(error_msg)
            
            # Log the error
            try:
                self.log_message(
                    log_project if 'log_project' in locals() else 'unknown',
                    'ERROR',
                    error_msg
                )
            except:
                pass  # Don't fail if logging fails
            
            return False