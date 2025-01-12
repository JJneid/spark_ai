# demo.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import subprocess
import time
import sys
import datetime
from decimal import Decimal

def start_api_server():
    """Start the FastAPI server in a separate process"""
    api_process = subprocess.Popen([sys.executable, "app.py"])
    time.sleep(2)
    return api_process

def create_sample_data(spark):
    """Create a more complex sales dataset"""
    # Generate sample sales data
    sales_data = [
        # Format: date, product, category, region, sales_channel, units_sold, unit_price, discount_pct, customer_segment
        ("2024-01-01", "MacBook Pro", "Laptops", "North", "Online", 10, 1299.99, 0.00, "Business"),
        ("2024-01-01", "MacBook Pro", "Laptops", "South", "Retail", 5, 1299.99, 0.05, "Consumer"),
        ("2024-01-01", "Dell XPS", "Laptops", "East", "Online", 8, 1199.99, 0.10, "Business"),
        ("2024-01-02", "iPhone 15", "Phones", "West", "Retail", 20, 999.99, 0.00, "Consumer"),
        ("2024-01-02", "Samsung S24", "Phones", "North", "Online", 15, 899.99, 0.15, "Consumer"),
        ("2024-01-02", "iPad Pro", "Tablets", "South", "Online", 12, 799.99, 0.00, "Education"),
        ("2024-01-03", "Surface Pro", "Tablets", "East", "Retail", 7, 899.99, 0.10, "Business"),
        ("2024-01-03", "Galaxy Tab", "Tablets", "West", "Online", 9, 649.99, 0.05, "Consumer"),
        ("2024-01-03", "ThinkPad", "Laptops", "North", "Retail", 6, 1099.99, 0.00, "Business"),
        ("2024-01-04", "iPhone 15", "Phones", "South", "Online", 25, 999.99, 0.10, "Consumer"),
        ("2024-01-04", "MacBook Air", "Laptops", "East", "Online", 11, 999.99, 0.00, "Education"),
        ("2024-01-04", "Surface Laptop", "Laptops", "West", "Retail", 8, 899.99, 0.05, "Consumer"),
        ("2024-01-05", "iPad Mini", "Tablets", "North", "Online", 14, 499.99, 0.00, "Consumer"),
        ("2024-01-05", "Galaxy S24+", "Phones", "South", "Retail", 18, 1099.99, 0.10, "Consumer"),
        ("2024-01-05", "Dell XPS", "Laptops", "East", "Online", 9, 1199.99, 0.15, "Business"),
    ]

    # Create DataFrame with the sample data
    df = spark.createDataFrame(sales_data, [
        "date", "product", "category", "region", "sales_channel", 
        "units_sold", "unit_price", "discount_pct", "customer_segment"
    ])

    # Add calculated columns
    df = df.withColumn("gross_sales", col("units_sold") * col("unit_price")) \
          .withColumn("discount_amount", col("gross_sales") * col("discount_pct")) \
          .withColumn("net_sales", col("gross_sales") - col("discount_amount"))

    return df

def main():
    # Start the API server
    print("🚀 Starting SlashML API server...")
    api_process = start_api_server()

    try:
        # Initialize Spark
        print("✨ Initializing Spark...")
        spark = SparkSession.builder \
            .appName("SlashMLSparkAI-Local") \
            .master("local[*]") \
            .getOrCreate()

        # Import and setup SlashML SparkAI
        from slashml_sparkai import setup_slashml_spark_ai
        
        # Initialize with local endpoint
        spark_ai = setup_slashml_spark_ai(
            spark, 
            endpoint_url="http://localhost:8000/v1/chat/completions"
        )

        # Create sample DataFrame
        print("📊 Creating sample DataFrame...")
        df = create_sample_data(spark)

        print("\n📝 Sample data schema:")
        df.printSchema()
        
        print("\n📝 Sample records:")
        df.show(5, truncate=False)

        print("\n📊 Available columns for analysis:")
        print("- Dimensions: date, product, category, region, sales_channel, customer_segment")
        print("- Metrics: units_sold, unit_price, discount_pct, gross_sales, discount_amount, net_sales")

        # Interactive query loop
        while True:
            print("\n🤔 Enter your question (or 'exit' to quit):")
            print("Example questions:")
            print("1. What are the top 3 products by net sales?")
            print("2. Show total sales by region and category")
            print("3. What's the average discount percentage by sales channel?")
            print("4. Which customer segment has the highest gross sales?")
            
            question = input("> ")
            
            if question.lower() in ['exit', 'quit', 'q']:
                break
                
            result = df.ai.transform(question)
            print("\n📊 Result:")
            result.show()

    finally:
        # Cleanup
        print("\n🧹 Cleaning up...")
        api_process.terminate()
        api_process.wait()
        spark.stop()

if __name__ == "__main__":
    main()
