query = """
SELECT  L_LINESTATUS,
        sum(L_QUANTITY) as SUM_QTY,
FROM    lineitem
GROUP BY L_LINESTATUS
"""
