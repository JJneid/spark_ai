# slashml_sparkai.py
from pyspark.sql import DataFrame, SparkSession
import requests
from typing import Optional
import hashlib

class SlashMLSparkAI:
    """
    SlashML's natural language to SparkSQL transformer.
    Allows querying Spark DataFrames using natural language.
    """
    
    def __init__(self, spark: SparkSession, endpoint_url: str = "http://localhost:8000/v1/chat/completions"):
        self._spark = spark
        self._endpoint_url = endpoint_url
        
    def generate_sql_query(self, question: str, schema: str) -> str:
        """Generate SQL query using SlashML's text-to-SQL model"""
        headers = {
            "Content-Type": "application/json"
        }
        
        base_prompt = """### Task
Generate a SQL query to answer [QUESTION]{question}[/QUESTION]

### Instructions
- If you cannot answer the question with the available database schema, return 'I do not know'

### Database Schema
{schema}

### Answer
Given the database schema, here is the SQL query that answers [QUESTION]{question}[/QUESTION]
[SQL]
"""
        formatted_prompt = base_prompt.format(question=question, schema=schema)
        
        data = {
            "model": "slashml/text-to-sql",
            "messages": [
                {"role": "user", "content": formatted_prompt}
            ]
        }
        
        try:
            response = requests.post(self._endpoint_url, headers=headers, json=data)
            response.raise_for_status()
            return response.json()['choices'][0]['message']['content']
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error calling SlashML API: {str(e)}")
    
    def _get_temp_view_schema(self, df: DataFrame) -> str:
        """Convert Spark DataFrame schema to CREATE TABLE format"""
        fields = []
        for field in df.schema.fields:
            # Map Spark types to SQL types
            type_mapping = {
                'string': 'VARCHAR',
                'integer': 'INTEGER',
                'long': 'BIGINT',
                'double': 'DOUBLE PRECISION',
                'boolean': 'BOOLEAN',
                'timestamp': 'TIMESTAMP',
                'date': 'DATE',
                'decimal': 'DECIMAL',
                'float': 'FLOAT'
            }
            spark_type = str(field.dataType).lower().split('[')[0]
            sql_type = type_mapping.get(spark_type, 'VARCHAR')
            fields.append(f"{field.name} {sql_type}")
            
        schema = "CREATE TABLE current_view (\n    " + ",\n    ".join(fields) + "\n);"
        return schema
    
    def activate(self):
        """Activate SlashML SparkAI functionality for all DataFrames"""
        DataFrame.ai = property(lambda df: SlashMLDataFrameAI(df, self))
        print("✨ SlashML SparkAI activated! You can now use .ai.transform() with natural language queries")

class SlashMLDataFrameAI:
    def __init__(self, df: DataFrame, spark_ai: SlashMLSparkAI):
        self._df = df
        self._spark_ai = spark_ai
        
    def transform(self, question: str) -> DataFrame:
        """Transform DataFrame using natural language query"""
        # Create a unique view name using hash
        view_name = "slashml_temp_view_" + hashlib.md5(question.encode()).hexdigest()[:12]
        self._df.createOrReplaceTempView(view_name)
        
        try:
            # Get schema and generate query
            schema = self._spark_ai._get_temp_view_schema(self._df)
            sql_query = self._spark_ai.generate_sql_query(question, schema)
            
            # Execute the generated query
            if sql_query and not sql_query.lower().startswith('i do not know'):
                print("\n🤖 Generated SQL Query:")
                print("------------------------")
                print(sql_query)
                print("------------------------")
                
                # Replace the temporary view name in the query
                sql_query = sql_query.replace("current_view", view_name)
                result = self._df.sparkSession.sql(sql_query)
                
                return result
            else:
                print("❌ Could not generate a valid SQL query for your question.")
                return self._df
            
        except Exception as e:
            print(f"❌ Error during transformation: {str(e)}")
            return self._df
            
        finally:
            # Clean up
            self._df.sparkSession.catalog.dropTempView(view_name)

def setup_slashml_spark_ai(spark: SparkSession, endpoint_url: Optional[str] = None) -> SlashMLSparkAI:
    """Initialize and activate SlashML SparkAI"""
    if endpoint_url:
        spark_ai = SlashMLSparkAI(spark, endpoint_url)
    else:
        spark_ai = SlashMLSparkAI(spark)
    spark_ai.activate()
    return spark_ai