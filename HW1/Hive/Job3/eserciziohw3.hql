SELECT t1.productid AS item1, t2.productid AS item2, COUNT(1) AS cnt FROM
	(SELECT DISTINCT productid, userid FROM reviews) t1
	JOIN
	(SELECT DISTINCT productid, userid FROM reviews) t2
	ON (t1.userid = t2.userid)
GROUP BY t1.productid, t2.productid
HAVING t1.productid != t2.productid
ORDER BY t1.productid DESC;
