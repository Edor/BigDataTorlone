CREATE TABLE IF NOT EXISTS year_score_average AS
	SELECT 
		productId,
		year,
		AVG(score) as average
	FROM
	(
		SELECT substr(from_unixtime(r.time),0,4) as year, r.score, r.productId
		FROM reviews r
	) t
	GROUP BY productId, year;

SELECT DISTINCT r.productId, COALESCE(t2003.average, 'NA'),  COALESCE(t2004.average, 'NA'),  COALESCE(t2005.average, 'NA'),  COALESCE(t2006.average, 'NA'),  COALESCE(t2007.average, 'NA'),  COALESCE(t2008.average, 'NA'),  COALESCE(t2009.average, 'NA'),  COALESCE(t2010.average, 'NA'),  COALESCE(t2011.average, 'NA'),  COALESCE(t2012.average, 'NA') FROM
	reviews r
	LEFT JOIN
	(SELECT productId, average FROM year_score_average WHERE year == 2003) t2003
	ON r.productId = t2003.productId
	LEFT JOIN
	(SELECT productId, average FROM year_score_average WHERE year == 2004) t2004
	ON r.productId = t2004.productId
	LEFT JOIN
	(SELECT productId, average FROM year_score_average WHERE year == 2005) t2005
	ON r.productId = t2005.productId
	LEFT JOIN
	(SELECT productId, average FROM year_score_average WHERE year == 2006) t2006
	ON r.productId = t2006.productId
	LEFT JOIN
	(SELECT productId, average FROM year_score_average WHERE year == 2007) t2007
	ON r.productId = t2007.productId
	LEFT JOIN
	(SELECT productId, average FROM year_score_average WHERE year == 2008) t2008
	ON r.productId = t2008.productId
	LEFT JOIN
	(SELECT productId, average FROM year_score_average WHERE year == 2009) t2009
	ON r.productId = t2009.productId
	LEFT JOIN
	(SELECT productId, average FROM year_score_average WHERE year == 2010) t2010
	ON r.productId = t2010.productId
	LEFT JOIN
	(SELECT productId, average FROM year_score_average WHERE year == 2011) t2011
	ON r.productId = t2011.productId
	LEFT JOIN
	(SELECT productId, average FROM year_score_average WHERE year == 2012) t2012
	ON r.productId = t2012.productId
ORDER BY r.productId;
