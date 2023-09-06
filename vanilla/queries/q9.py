query = """
SELECT  L_SHIPMODE,
        sum(L_QUANTITY) as SUM_QTY,
FROM    lineitem
GROUP BY L_SHIPMODE
"""
