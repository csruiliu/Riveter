query = """
SELECT  P_NAME, PS_AVAILQTY, S_ACCTBAL
FROM  	partsupp,
        part,
        supplier
WHERE	PS_PARTKEY = P_PARTKEY
        AND	PS_SUPPKEY = S_SUPPKEY
"""
