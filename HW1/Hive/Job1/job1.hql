SELECT year, word, count
FROM 
	(SELECT year, word, count, row_number() OVER (PARTITION BY year ORDER BY count DESC) AS rank 
	FROM 
		(SELECT year, word, SUM(count) as count
		FROM
			(SELECT substr(from_unixtime(r.time),0,4) as year, exp.word as word, COUNT(*) as count 
			FROM 
				reviews r
	 			LATERAL VIEW explode(split(r.summary,'\\s+')) exp AS word
 			GROUP BY time,word) h
		WHERE year != 'NULL' and year > 1970
		GROUP BY year,word
		ORDER BY count DESC) b
	) t
WHERE rank < 11
ORDER BY year DESC, count DESC;
