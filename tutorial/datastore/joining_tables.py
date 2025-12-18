from cosmotech.coal.store.native_python import store_pylist
from cosmotech.coal.store.store import Store

store = Store(reset=True)

# Store customer data
customers = [
    {"customer_id": 1, "name": "Acme Corp", "segment": "Enterprise"},
    {"customer_id": 2, "name": "Small Shop", "segment": "SMB"},
    {"customer_id": 3, "name": "Tech Giant", "segment": "Enterprise"},
]
store_pylist("customers", customers, store=store)

# Store order data
orders = [
    {"order_id": 101, "customer_id": 1, "amount": 5000},
    {"order_id": 102, "customer_id": 2, "amount": 500},
    {"order_id": 103, "customer_id": 1, "amount": 7500},
    {"order_id": 104, "customer_id": 3, "amount": 10000},
]
store_pylist("orders", orders, store=store)

# Join tables to analyze orders by customer segment
results = store.execute_query(
    """
    SELECT c.segment, COUNT(o.order_id) as order_count, SUM(o.amount) as total_revenue
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.segment
"""
).to_pylist()

print(results)
# > [{'segment': 'Enterprise', 'order_count': 3, 'total_revenue': 22500}, {'segment': 'SMB', 'order_count': 1, 'total_revenue': 500}]
