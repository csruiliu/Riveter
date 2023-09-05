query = """
SELECT  N_NAME, R_NAME
FROM  	nation,
        region
WHERE	N_REGIONKEY = R_REGIONKEY
"""
