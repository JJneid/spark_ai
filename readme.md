# SlashML SparkAI Integration

A text-to-SQL integration that extends PySpark's DataFrame API with natural language query capabilities using SlashML.

#
## 🔍 Example Queries

```python
# Revenue analysis
df.ai.transform("Show me total revenue by category")

# Price analysis
df.ai.transform("Which category has the highest average price?")

# Product filtering
df.ai.transform("Show products with price above average")

# Custom aggregations
df.ai.transform("Calculate total units sold by category")
```

# 🚀 Features

- Natural language to SQL query conversion
- Seamless integration with PySpark DataFrame API
- Simple REST API interface
- Extensible LLM architecture
- Comprehensive error handling
- Easy-to-use Python interface

## 📋 Prerequisites

- Python 3.7+
- PySpark 3.0+
- Java 8+ (for PySpark)

## 🛠️ Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/slashml-spark-ai.git
cd slashml-spark-ai
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## 📦 Project Structure

```
slashml-spark-ai/
├── pyspark_ai/
│   ├── __init__.py
│   └── llms/
│       ├── __init__.py
│       ├── base.py
│       └── slashml.py
├── examples/
│   └── demo.py
├── app.py
├── requirements.txt
└── setup.py
```

## 🚀 Quick Start

1. Start the SlashML API server:
```bash
python app.py
```

2. In a new terminal, run the demo:
```bash
python examples/demo.py
```

## 💻 Usage Example

```python
from pyspark.sql import SparkSession
from pyspark_ai import SparkAI
from pyspark_ai.llms import SlashMLLLM

# Initialize Spark
spark = SparkSession.builder.appName("SlashML-SparkAI").getOrCreate()

# Create sample DataFrame
data = [
    ("MacBook Pro", "Laptops", 1299.99, 50),
    ("iPhone 15", "Phones", 999.99, 100),
    ("iPad Pro", "Tablets", 799.99, 75)
]
df = spark.createDataFrame(data, ["product", "category", "price", "units_sold"])

# Initialize SparkAI with SlashML
llm = SlashMLLLM(endpoint_url="http://localhost:8000/v1/chat/completions")
spark_ai = SparkAI(llm=llm)
spark_ai.activate()

# Use natural language queries
result = df.ai.transform("What are the top 3 products by price?")
result.show()
```



## 🔧 Configuration

The SlashML LLM can be configured with the following parameters:

```python
llm = SlashMLLLM(
    endpoint_url="http://localhost:8000/v1/chat/completions",  # SlashML API endpoint
)
```

## 🛠️ Development

1. Format code:
```bash
black .
```

2. Run linter:
```bash
flake8 .
```

3. Run tests:
```bash
pytest tests/
```

## 📝 API Documentation

### SlashMLLLM

Main class for SlashML integration with SparkAI.

```python
class SlashMLLLM(LLM):
    def __init__(self, endpoint_url: str = "http://localhost:8000/v1/chat/completions"):
        """
        Initialize SlashML LLM.
        
        Args:
            endpoint_url: URL of the SlashML API endpoint
        """
        
    def generate_sql(self, question: str, schema: str) -> str:
        """
        Generate SQL from natural language question.
        
        Args:
            question: Natural language question
            schema: Database schema in CREATE TABLE format
            
        Returns:
            Generated SQL query
        """
```

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgements

- [SparkAI](https://github.com/databrickslabs/spark-ai) for the base architecture
- [PySpark](https://spark.apache.org/docs/latest/api/python/) for the DataFrame API
- [FastAPI](https://fastapi.tiangolo.com/) for the API server

## 📧 Contact

Your Name - youremail@example.com

Project Link: [https://github.com/yourusername/slashml-spark-ai](https://github.com/yourusername/slashml-spark-ai)