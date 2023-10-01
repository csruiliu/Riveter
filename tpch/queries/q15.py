query = """
SELECT	S_SUPPKEY,
		S_NAME,
		S_ADDRESS,
		S_PHONE,
		TOTAL_REVENUE
FROM	supplier,
        (       
            SELECT	L_SUPPKEY as SUPPLIER_NO,
                    sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as TOTAL_REVENUE,
            FROM    lineitem
            WHERE   L_SHIPDATE >= '1996-01-01'
                    AND L_SHIPDATE < '1996-04-01'
            GROUP BY    L_SUPPKEY
        )
WHERE	S_SUPPKEY = SUPPLIER_NO
		AND TOTAL_REVENUE = (
			SELECT	MAX(TOTAL_REVENUE)
			FROM	(
			            SELECT	L_SUPPKEY as SUPPLIER_NO,
                        sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as TOTAL_REVENUE,
                        FROM    lineitem
                        WHERE   L_SHIPDATE >= '1996-01-01'
                                AND L_SHIPDATE < '1996-04-01'
                        GROUP BY    L_SUPPKEY
			        )   
		)
ORDER BY	S_SUPPKEY
"""