query = """
SELECT  sum(L_QUANTITY) as REVENUE
FROM    lineitem
ORDER BY    REVENUE
"""
