# app.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import uvicorn
import re

class Message(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    model: str
    messages: List[Message]

class ChatResponse(BaseModel):
    id: str = "local-response"
    object: str = "chat.completion"
    created: int = 1234567890
    choices: List[dict]

app = FastAPI(title="SlashML Local API")

def extract_question(prompt: str) -> str:
    """Extract the question from between [QUESTION] tags"""
    match = re.search(r'\[QUESTION\](.*?)\[/QUESTION\]', prompt, re.DOTALL)
    if match:
        return match.group(1).strip().lower()
    return prompt.lower()

def generate_sql(prompt: str) -> str:
    """
    Enhanced SQL generation logic with support for more complex queries.
    Replace this with your actual model.
    """
    question = extract_question(prompt)
    
    # Pattern matching for different types of queries
    if any(word in question for word in ['top', 'highest', 'best']):
        if 'by region' in question:
            return """
            SELECT region, product, category, SUM(net_sales) as total_net_sales
            FROM current_view
            GROUP BY region, product, category
            ORDER BY total_net_sales DESC
            LIMIT 5
            """
        elif 'by category' in question:
            return """
            SELECT category, SUM(net_sales) as total_net_sales
            FROM current_view
            GROUP BY category
            ORDER BY total_net_sales DESC
            """
        else:
            return """
            SELECT product, category, 
                   SUM(units_sold) as total_units,
                   SUM(net_sales) as total_net_sales
            FROM current_view
            GROUP BY product, category
            ORDER BY total_net_sales DESC
            LIMIT 3
            """
            
    elif 'average' in question or 'avg' in question:
        if 'discount' in question:
            return """
            SELECT sales_channel,
                   ROUND(AVG(discount_pct) * 100, 2) as avg_discount_percentage,
                   ROUND(AVG(discount_amount), 2) as avg_discount_amount
            FROM current_view
            GROUP BY sales_channel
            ORDER BY avg_discount_percentage DESC
            """
        else:
            return """
            SELECT category,
                   ROUND(AVG(unit_price), 2) as avg_price,
                   ROUND(AVG(net_sales), 2) as avg_sales
            FROM current_view
            GROUP BY category
            ORDER BY avg_sales DESC
            """
            
    elif 'sales by region' in question:
        return """
        SELECT region,
               category,
               SUM(gross_sales) as total_gross_sales,
               SUM(discount_amount) as total_discounts,
               SUM(net_sales) as total_net_sales
        FROM current_view
        GROUP BY region, category
        ORDER BY region, total_net_sales DESC
        """
        
    elif 'customer segment' in question:
        return """
        SELECT customer_segment,
               COUNT(DISTINCT product) as unique_products,
               SUM(units_sold) as total_units,
               ROUND(SUM(gross_sales), 2) as total_gross_sales,
               ROUND(AVG(discount_pct) * 100, 2) as avg_discount_pct
        FROM current_view
        GROUP BY customer_segment
        ORDER BY total_gross_sales DESC
        """
        
    else:
        return """
        SELECT category,
               COUNT(DISTINCT product) as unique_products,
               SUM(units_sold) as total_units,
               ROUND(SUM(net_sales), 2) as total_net_sales
        FROM current_view
        GROUP BY category
        ORDER BY total_net_sales DESC
        """

@app.post("/v1/chat/completions")
async def create_chat_completion(request: ChatRequest) -> ChatResponse:
    try:
        prompt = next(msg.content for msg in reversed(request.messages) 
                     if msg.role == "user")
        sql_query = generate_sql(prompt)
        
        return ChatResponse(
            choices=[{
                "message": {
                    "role": "assistant",
                    "content": sql_query
                },
                "index": 0,
                "finish_reason": "stop"
            }]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
