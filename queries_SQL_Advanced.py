import sqlite3
import pandas as pd

connection = sqlite3.connect('data/ecommerce.sqlite')
connection.row_factory = sqlite3.Row
cursor = connection.cursor()


def database_explorer(db_cursor):
    """ Shows the number of tables in the database"""

    query = """
            SELECT name FROM sqlite_master  
            WHERE type='table';
            """
    db_cursor.execute(query)
    rows = db_cursor.fetchall()

    #     print(f"""
    # {type(rows) = }
    # {len(rows) = }
    # """)

    return [row[row.keys()[0]] for row in rows]


def query_orders(db_cursor):
    """return a list of orders displaying each column"""
    query = '''
            SELECT *
            FROM orders
            '''
    db_cursor.execute(query)
    rows = db_cursor.fetchall()
    # print(f"""
    # {type(rows) = } / {len(rows) = }
    # {type(rows) = }
    # {[row.keys()for row in rows][0] = }
    # """)

    orders_table_keys = [row.keys() for row in rows][0]

    for index, row in enumerate(rows[::]):
        print(f"""
        CHECKING ROW CONTENT | row_{index + 1}:""")
        for key in orders_table_keys:
            print(f"""{key}: {row[key]}""")

    return rows


def get_orders_range(db_cursor, date_from, date_to):
    """ return a list of orders displaying all columns with OrderDate between
    date_from and date_to (excluding date_from and including date_to)"""

    query = '''
            SELECT *
            FROM orders
            WHERE OrderDate > ?
            AND OrderDate <= ?
            '''
    db_cursor.execute(query, (date_from, date_to))
    rows = db_cursor.fetchall()

    print(f"""
    {type(rows) = } / {len(rows) = }
    {type(rows) = }
    {[row.keys()for row in rows][0] = }
    """)

    orders_table_keys = [row.keys() for row in rows][0]

    for index, row in enumerate(rows[::]):
        print(f"""
        CHECKING ROW CONTENT | row_{index + 1}:""")
        for key in orders_table_keys:
            print(f"""{key}: {row[key]}""")
    return rows


def get_waiting_time(db_cursor):
    """get a list with all the orders displaying each column
    and calculate an extra TimeDelta column displaying the number of days
    between OrderDate and ShippedDate, ordered by ascending TimeDelta"""

    query = '''
            SELECT
            *,
            julianday(orders.ShippedDate) - julianday(orders.OrderDate) AS TimeDelta
            FROM orders
            ORDER BY TimeDelta
            '''
    db_cursor.execute(query)
    rows = db_cursor.fetchall()

    print(f"""
    {type(rows) = } / {len(rows) = }
    {type(rows) = }
    {[row.keys()for row in rows][0] = }
    """)

    orders_table_keys = [row.keys() for row in rows][0]

    for index, row in enumerate(rows[::]):
        print(f"""
        CHECKING ROW CONTENT | row_{index + 1}:""")
        for key in orders_table_keys:
            print(f"""{key}: {row[key]}""")
    return rows


def detailed_orders(db_cursor):
    """return a list of all orders (order_id, customer.contact_name,
    employee.firstname) ordered by order_id"""
    query = '''
            SELECT
            orders.OrderID,
            customers.ContactName,
            employees.FirstName
            FROM orders
            JOIN customers ON orders.CustomerID = customers.CustomerID
            JOIN employees ON orders.EmployeeID = employees.EmployeeID
            ORDER BY orders.OrderID
            '''
    rows = db_cursor.execute(query).fetchall()
    print(f"""
    {type(rows) = } / {len(rows) = }
    {type(rows) = }
    {[row.keys()for row in rows][0] = }
    """)

    orders_table_keys = [row.keys() for row in rows][0]

    for index, row in enumerate(rows[::]):
        print(f"""
        CHECKING ROW CONTENT | row_{index + 1}:""")
        for key in orders_table_keys:
            print(f"""{key}: {row[key]}""")
    return rows


def spent_per_customer(db_cursor):
    """return the total amount spent per customer ordered by ascending total
    amount (to 2 decimal places)
    Example :
        Jean   |   100
        Marc   |   110
        Simon  |   432
        ...
    """
    query = '''
            SELECT
            Customers.ContactName,
            ROUND(SUM(details.UnitPrice * details.Quantity)) AS cumulative_amount
            FROM OrderDetails AS details
            JOIN Orders ON details.OrderID = Orders.OrderId
            JOIN Customers ON Orders.CustomerID = Customers.CustomerID
            GROUP BY ContactName
            ORDER BY cumulative_amount
            '''
    rows = db_cursor.execute(query).fetchall()
    print(f"""
    {type(rows) = } / {len(rows) = }
    {type(rows) = }
    {[row.keys()for row in rows][0] = }
    """)

    orders_table_keys = [row.keys() for row in rows][0]

    for index, row in enumerate(rows[::]):
        print(f"""
        CHECKING ROW CONTENT | row_{index + 1}:""")
        for key in orders_table_keys:
            print(f"""{key}: {row[key]}""")

    return rows


def best_employee(db_cursor):
    """Implement the best_employee method to determine who’s the best employee! By “best employee”, we mean the one who
    sells the most.
    We expect the function to return a tuple like: ('FirstName', 'LastName', 6000 (the sum of all purchase)). The order
    of the information is irrelevant"""
    query = '''
        SELECT
            Employees.FirstName,
            Employees.LastName,
            SUM(details.UnitPrice * details.Quantity) AS cumulative_amount
        FROM OrderDetails AS details
        JOIN Orders ON details.OrderID = Orders.OrderID
        JOIN Employees ON Employees.EmployeeID = Orders.EmployeeID
        GROUP BY Employees.EmployeeID
        ORDER BY cumulative_amount DESC
        LIMIT 1
    '''
    rows = db_cursor.execute(query).fetchall()
    print(f"""
        {type(rows) = } / {len(rows) = }
        {type(rows) = }
        {[row.keys()for row in rows][0] = }
        """)

    orders_table_keys = [row.keys() for row in rows][0]

    for index, row in enumerate(rows[::]):
        print(f"""
            CHECKING ROW CONTENT | row_{index + 1}:""")
        for key in orders_table_keys:
            print(f"""{key}: {row[key]}""")

    return rows


def orders_per_customer(db_cursor):
    """Return a list of tuples where each tuple contains the contactName
    of the customer and the number of orders they made (contactName,
    number_of_orders). Order the list by ascending number of orders"""
    query = '''
            SELECT
            Customers.ContactName,
            COUNT(Orders.OrderID) AS order_amount
            FROM Customers
            LEFT JOIN Orders ON Customers.CustomerID = Orders.CustomerID
            GROUP BY Customers.CustomerID
            ORDER BY order_amount ASC
            '''
    rows = db_cursor.execute(query).fetchall()
    print(f"""
            {type(rows) = } / {len(rows) = }
            {type(rows) = }
            {[row.keys()for row in rows][0] = }
            """)

    orders_table_keys = [row.keys() for row in rows][0]

    for index, row in enumerate(rows[::]):
        print(f"""
                CHECKING ROW CONTENT | row_{index + 1}:""")
        for key in orders_table_keys:
            print(f"""{key}: {row[key]}""")

    return rows


def order_rank_per_customer(db_cursor):
    """
    Implement order_rank_per_customer to rank the orders of each customer according to the order date.
    For each customer, the orders should be ranked in the chronological order.
    This function should return a list of tuples like (OrderID, CustomerID, OrderDate, OrderRank).
    """
    query = """
            SELECT
                OrderID,    
                CustomerID,
                OrderDate,
                RANK() OVER (
                            PARTITION BY CustomerID
                            ORDER BY OrderDate
                            ) AS OrderRank
            FROM Orders
            """
    rows = db_cursor.execute(query).fetchall()
    print(f"""
{type(rows) = } / {len(rows) = }
{type(rows) = }
{[row.keys()for row in rows][0] = }
""")

    orders_table_keys = [row.keys() for row in rows][0]

    for index, row in enumerate(rows[::]):
        print(f"""
CHECKING ROW CONTENT | row_{index + 1}:""")
        for key in orders_table_keys:
            print(f"""{key}: {row[key]}""")

    return rows


def order_cumulative_amount_per_customer(db_cursor):
    """
    Implement order_cumulative_amount_per_customer to compute the cumulative amount (in USD) of the orders of each
    customer according to the order date.
    For each customer, the orders should be ranked in the chronological order.
    This function should return a list of tuples like (OrderID, CustomerID, OrderDate, OrderCumulativeAmount).
    """
    query = """
        SELECT
            Orders.OrderID,
            Orders.CustomerID,
            Orders.OrderDate,
            SUM(SUM(OrderDetails.UnitPrice * OrderDetails.Quantity)) OVER(PARTITION BY Orders.CustomerID ORDER BY Orders.OrderDate) OrderCumulativeAmount
        FROM Orders
        JOIN OrderDetails ON Orders.OrderID = OrderDetails.OrderID
        GROUP BY Orders.OrderID
        ORDER BY Orders.CustomerID
    """

    rows = db_cursor.execute(query).fetchall()
    print(f"""
    {type(rows) = } / {len(rows) = }
    {type(rows) = }
    {[row.keys()for row in rows][0] = }
    """)

    orders_table_keys = [row.keys() for row in rows][0]

    for index, row in enumerate(rows[::]):
        print(f"""
CHECKING ROW CONTENT | row_{index + 1}:""")
        for key in orders_table_keys:
            print(f"""{key}: {row[key]}""")

    return rows


def get_average_purchase(db_cursor):
    """return the average amount spent per order for each customer ordered by customer ID"""
    query = '''
        WITH OrderValues AS (
          SELECT
            SUM(od.UnitPrice * od.Quantity) AS value,
            od.OrderID
          FROM OrderDetails od
          GROUP BY od.OrderID
        )
        SELECT
            c.CustomerID,
            ROUND(AVG(ov.value), 2) AS average
        FROM Customers c
        JOIN Orders o ON c.CustomerID = o.CustomerID
        JOIN OrderValues ov ON ov.OrderID = o.OrderID
        GROUP BY c.CustomerID
        ORDER BY c.CustomerID
    '''
    rows = db_cursor.execute(query).fetchall()
    print(f"""
        {type(rows) = } / {len(rows) = }
        {type(rows) = }
        {[row.keys()for row in rows][0] = }
        """)

    orders_table_keys = [row.keys() for row in rows][0]

    for index, row in enumerate(rows[::]):
        print(f"""
CHECKING ROW CONTENT | row_{index + 1}:""")
        for key in orders_table_keys:
            print(f"""{key}: {row[key]}""")
    return db_cursor.execute(query).fetchall()


def get_general_avg_order(db_cursor):
    """return the average amount spent per order"""

    query = '''
        WITH OrderValues AS (
          SELECT SUM(od.Quantity * od.UnitPrice) AS value
          FROM OrderDetails od
          GROUP BY od.OrderID
        )
        SELECT ROUND(AVG(ov.value), 2) AS AverageAmountOrder
        FROM OrderValues ov
    '''
    rows = db_cursor.execute(query).fetchall()
    print(f"""
{type(rows) = } / {len(rows) = }
{type(rows) = }
{[row.keys()for row in rows][0] = }
""")

    orders_table_keys = [row.keys() for row in rows][0]

    for index, row in enumerate(rows[::]):
        print(f"""
CHECKING ROW CONTENT | row_{index + 1}:""")
        for key in orders_table_keys:
            print(f"""{key}: {row[key]}""")
    return db_cursor.execute(query).fetchall()


def best_customers(db_cursor):
    """return the customers who have an average purchase greater than the general average purchase"""

    query = '''
        WITH OrderValues AS (
          SELECT
            SUM(od.UnitPrice * od.Quantity) AS value,
            od.OrderID
          FROM OrderDetails od
          GROUP BY od.OrderID
        ),
        GeneralOrderValue AS (
          SELECT ROUND(AVG(ov.value), 2) AS average
          FROM OrderValues ov
        )
        SELECT
          c.CustomerID,
          ROUND(AVG(ov.value),2) AS avg_amount_per_customer
        FROM Customers c
        JOIN Orders o ON o.CustomerID = c.CustomerID
        JOIN OrderValues ov ON ov.OrderID = o.OrderID
        GROUP BY c.CustomerID
        HAVING AVG(ov.value) > (SELECT average FROM GeneralOrderValue)
        ORDER BY avg_amount_per_customer DESC
    '''
    rows = db_cursor.execute(query).fetchall()
    print(f"""
{type(rows) = } / {len(rows) = }
{type(rows) = }
{[row.keys()for row in rows][0] = }
""")

    orders_table_keys = [row.keys() for row in rows][0]

    for index, row in enumerate(rows[::]):
        print(f"""
CHECKING ROW CONTENT | row_{index + 1}:""")
        for key in orders_table_keys:
            print(f"""{key}: {row[key]}""")
    return db_cursor.execute(query).fetchall()


def top_ordered_product_per_customer(db_cursor):
    """return the list of the top ordered product by each customer
    based on the total ordered amount in USD"""
    query = """
        WITH OrderedProducts AS (
            SELECT
                CustomerID,
                ProductID, SUM(OrderDetails.Quantity * OrderDetails.UnitPrice) AS ProductValue
            FROM OrderDetails
            JOIN Orders ON OrderDetails.OrderID = Orders.OrderID
            GROUP BY Orders.CustomerID, OrderDetails.ProductID
            ORDER BY ProductValue DESC
        ),
        ranks AS (
        SELECT
            OrderedProducts.CustomerID,
            OrderedProducts.ProductID,
            OrderedProducts.ProductValue,
            RANK() OVER(PARTITION BY OrderedProducts.CustomerID ORDER BY OrderedProducts.ProductValue DESC) as order_rank
            FROM OrderedProducts
            )
        SELECT ranks.CustomerID,ranks.ProductID, ranks.ProductValue
        from ranks
        WHERE order_rank = 1
        ORDER BY ranks.ProductValue DESC
    """
    rows = db_cursor.execute(query).fetchall()
    print(f"""
{type(rows) = } / {len(rows) = }
{type(rows) = }
{[row.keys()for row in rows][0] = }
""")

    orders_table_keys = [row.keys() for row in rows][0]

    for index, row in enumerate(rows[::]):
        print(f"""
CHECKING ROW CONTENT | row_{index + 1}:""")
        for key in orders_table_keys:
            print(f"""{key}: {row[key]}""")
    return db_cursor.execute(query).fetchall()


def average_number_of_days_between_orders(db_cursor):
    """return the average number of days between two consecutive orders of the same customer"""
    query = """
        WITH DatedOrders AS (
            SELECT
                CustomerID,
                OrderID,
                OrderDate,
                LAG(OrderDate, 1, 0) OVER (
                    PARTITION BY CustomerID
                    ORDER By OrderDate
                ) PreviousOrderDate
            FROM Orders
        )
        SELECT ROUND(AVG(JULIANDAY(OrderDate) - JULIANDAY(PreviousOrderDate))) AS delta
        FROM DatedOrders
        WHERE PreviousOrderDate != 0
    """
    rows = db_cursor.execute(query).fetchall()
    print(f"""
{type(rows) = } / {len(rows) = }
{type(rows) = }
{[row.keys()for row in rows][0] = }
""")

    orders_table_keys = [row.keys() for row in rows][0]

    for index, row in enumerate(rows[::]):
        print(f"""
CHECKING ROW CONTENT | row_{index + 1}:""")
        for key in orders_table_keys:
            print(f"""{key}: {row[key]}""")
    return db_cursor.execute(query).fetchall()


def exporting_csv(db_cursor):
    query_exploring = """
            SELECT name FROM sqlite_master  
            WHERE type='table';
            """

    cursor.execute(query_exploring)
    rows = cursor.fetchall()
    print(rows)

    for index, row in enumerate(rows[1::]):
        print(f"""{index}_{row[0]}""")
        query_table = f"""
                SELECT * FROM {row[0]}  
                """
        print(query_table)
        movies = pd.read_sql(query_table, connection)
        movies.to_csv(f"data/csv_ecommerce_{row[0]}.csv", index=False)


print(f"""
-------------------------------------------------------------------------------------------
database_explorer(db_cursor=cursor) = 
query_orders(db_cursor=cursor) = 
get_orders_range(db_cursor= cursor, date_from= "2012-04-06", date_to="2012-09-04") = 
get_waiting_time(db_cursor=cursor) = 
detailed_orders(db_cursor=cursor) = 
spent_per_customer(db_cursor= cursor) = 
best_employee(db_cursor= cursor) = 
orders_per_customer(db_cursor= cursor) = 
order_rank_per_customer(db_cursor=cursor) = 
order_cumulative_amount_per_customer(db_cursor= cursor) = 
get_average_purchase(db_cursor=cursor) = 
get_general_avg_order(db_cursor=cursor) = 
best_customers(db_cursor= cursor) = 
top_ordered_product_per_customer(db_cursor=cursor) =
average_number_of_days_between_orders(db_cursor=cursor) = 
exporting_csv(db_cursor=cursor)
""")
