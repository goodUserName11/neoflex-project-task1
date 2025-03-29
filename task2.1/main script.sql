-- Найти дубликаты
SELECT client_rk, effective_from_date, COUNT(*)
FROM dm.client
GROUP BY client_rk, effective_from_date
HAVING COUNT(*) > 1;

-- Удаление дубликатов
BEGIN;
DELETE FROM dm.client a 
USING (
    SELECT MIN(ctid) as ctid, client_rk, effective_from_date
    FROM dm.client 
    GROUP BY client_rk, effective_from_date 
	HAVING COUNT(*) > 1
) b
WHERE a.client_rk = b.client_rk
	AND a.effective_from_date = b.effective_from_date
	AND a.ctid <> b.ctid
ROLLBACK;