from fastapi import FastAPI
from fastapi.responses import JSONResponse
from collections import defaultdict
import random
import string
from sqlalchemy import create_engine, text
import sqlalchemy as db

app = FastAPI();

DATABASE_URL = "mysql+mysqldb://root:12345Qq!@localhost:3306/classicmodels"
engine = create_engine(DATABASE_URL)
metadata = db.MetaData()
metadata.reflect(bind=engine)

@app.get("/cancelled_orders/{initialDate}/{finalDate}")
def cancelledOrders(initialDate: str, finalDate: str):
    try:

        sql_query = f"""
        SELECT 
            CONCAT(c.contactFirstName, ' ', c.contactLastName) AS full_name,
            p.productName AS product,
            od.quantityOrdered AS quantity,
            od.priceEach * od.quantityOrdered AS total_price,
            o.requiredDate AS cancel_date,
            o.comments AS comments
        FROM orders o
        INNER JOIN customers c ON o.customerNumber = c.customerNumber
        INNER JOIN orderdetails od ON o.orderNumber = od.orderNumber
        INNER JOIN products p ON od.productCode = p.productCode
        WHERE o.status = 'Cancelled' AND o.requiredDate BETWEEN '{initialDate}' AND '{finalDate}';
        """

        with engine.connect() as conn:
            response = conn.execute(text(sql_query))
            response_list = []

            for row in response:
                client_name = row[0]
                product = row[1]
                quantity = row[2]
                price = float(row[3]) 
                cancel_date = str(row[4]) 
                comments = row[5]

                order = {
                    "product": product,
                    "quantity": quantity,
                    "total_price": price
                }

                # Searchs if theres already a client with the same name in response_list
                existing_client = next((client for client in response_list if client["client_name"] == client_name), None)

                if existing_client:
                    existing_client["order"].append(order)
                else:
                    new_client = {
                        "client_name": client_name,
                        "cancel_date": cancel_date,
                        "comments": comments,
                        "order": [order]
                    }
                    response_list.append(new_client)
        
            response_count = len(response_list)

        json_response = {
            "results": response_count,
            "response": response_list
        }

        return JSONResponse(json_response)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/credit_used")
def creditUsed():
    sql_query = """
    SELECT 
        CONCAT(c.contactFirstName, ' ', c.contactLastName) AS full_name,
        MAX(o.orderDate) AS last_order_date,
        c.creditLimit AS credit_limit,
        SUM(DISTINCT p.amount) AS credit_used,
        CAST((SUM(DISTINCT p.amount) / c.creditLimit) * 100 AS UNSIGNED) AS credit_used_percentage
    FROM customers c
    JOIN payments p ON c.customerNumber = p.customerNumber
    JOIN orders o ON c.customerNumber = o.customerNumber
    GROUP BY c.customerNumber
    HAVING (SUM(DISTINCT p.amount) / c.creditLimit) * 100 >= 80;
    """
    try:
        with engine.connect() as conn:
            response = conn.execute(text(sql_query))
            response_list = []

            for row in response:
                client_name = row[0]
                last_order = str(row[1])
                credit_limit = float(row[2])
                credit_used = float(row[3])
                credit_used_percentage = int(row[4])

                client_data = {
                    "client_name": client_name,
                    "last_order_date": last_order,
                    "credit_limit": credit_limit,
                    "credit_used": credit_used,
                    "credit_used_percentage": credit_used_percentage
                }

                response_list.append(client_data)

            response_count = len(response_list)

        json_response = {
            "results": response_count,
            "response": response_list
        }

        return JSONResponse(json_response)
    
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/sales_by_country")
def salesByCountry():
    sql_query = """
    SELECT 
        c.country,
        YEAR(o.orderDate) AS year,
        SUM(od.quantityOrdered * od.priceEach) AS total_sales
    FROM customers c
    JOIN orders o ON c.customerNumber = o.customerNumber
    JOIN orderdetails od ON o.orderNumber = od.orderNumber
    GROUP BY c.country, YEAR(o.orderDate);
    """
    try:
        with engine.connect() as conn:
            response = conn.execute(text(sql_query))

            response_dict = defaultdict(list)

            for row in response:
                country = row[0]
                year = int(row[1])
                total_sales = float(row[2])

                sales_by_year = {
                    "year": year,
                    "total_sales": total_sales
                }

                response_dict[country].append(sales_by_year)

            response_list = [{"country": country, "sales_by_year": sales} for country, sales in response_dict.items()]
            response_count = len(response_list)

        json_response = {
            "results": response_count,
            "response": response_list
        }

        return JSONResponse(json_response)
    
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/credit_limit")
def creditLimit():
    sql_query = "SELECT CONCAT(contactFirstName, ' ', contactLastName) AS full_name, creditLimit FROM customers;"

    try:
        with engine.connect() as conn:
            response = conn.execute(text(sql_query))
            response_list = []

            for row in response:
                full_name, credit_limit = row
                credit_limit = float(credit_limit)
                response_list.append({"full_name": full_name, "creditLimit": credit_limit})

            response_count = len(response_list)

        json_response = {
            "results": response_count,
            "response": response_list
        }

        return JSONResponse(json_response)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.put("/rise_credit_limit")
def riseCreditLimit():
    sql_query = "UPDATE customers SET creditLimit = creditLimit * 1.05 WHERE customerNumber > 0;"

    try:
        with engine.connect() as conn:
            conn.execute(text(sql_query))
            conn.commit()
        
        return JSONResponse({"message": "Credit limit increased by 5% for all customers"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.put("/decrease_credit_limit")
def decreaseCreditLimit():
    sql_query = "UPDATE customers SET creditLimit = creditLimit / 1.05 WHERE customerNumber > 0;"

    try:
        with engine.connect() as conn:
            conn.execute(text(sql_query))
            conn.commit()
        
        return JSONResponse({"message": "Credit limit decreased by 5% for all customers"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/new_product")
def newProduct():
    product_table = metadata.tables['products']
    random_digits = ''.join(random.choices(string.digits, k=4))

    insert_prod = db.insert(product_table).values(  productCode = f"S00_{random_digits}",
                                                    productName="Added product",
                                                    productLine="Planes",
                                                    productScale="1:1",
                                                    productVendor="Data Warehouse",
                                                    productDescription="Just a dummy record for inserting endpoint",
                                                    quantityInStock=100,
                                                    buyPrice=25.00,
                                                    MSRP=25.00 )
    try:
        with engine.connect() as conn:
            conn.execute(insert_prod)
            conn.commit()
        return JSONResponse({"message": "Record created"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)