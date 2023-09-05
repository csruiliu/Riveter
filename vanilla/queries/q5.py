query = """
SELECT  C_CUSTKEY, C_NAME, O_ORDERKEY, O_ORDERSTATUS
FROM  	customer,
        orders
WHERE	C_CUSTKEY = O_CUSTKEY
"""
