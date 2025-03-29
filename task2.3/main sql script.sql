-- Не понял какой уже имеющийся запрос для выборки корректных значений account_in_sum/account_out_sum имеется в виду под пунктом 1, поэтому сделал оба варианта

-- Выборка корректных значений account_in_sum
SELECT 
	ab_today.account_rk,
	ab_today.effective_date,
-- 	ab_today.account_in_sum AS today_account_in_sum,
	COALESCE(ab_prev_day.account_out_sum, ab_today.account_in_sum) AS account_in_sum,
	ab_today.account_out_sum AS account_out_sum
FROM rd.account_balance AS ab_today
LEFT JOIN rd.account_balance AS ab_prev_day
	ON ab_today.account_rk = ab_prev_day.account_rk
	AND	ab_today.effective_date = ab_prev_day.effective_date + INTERVAL '1 day'
WHERE ab_today.account_in_sum <> ab_prev_day.account_out_sum

-- Исправление на корректные account_in_sum
BEGIN;
WITH ab_today_prev_day AS (
	SELECT 
-- 		Идентификатор счета
		ab_today.account_rk,
-- 		Дата
		ab_today.effective_date,
-- 		Сумма на начало дня
-- 		ab_today.account_in_sum AS account_in_sum,
		COALESCE(ab_prev_day.account_out_sum, ab_today.account_in_sum) AS account_in_sum,
-- 		Сумма на конец дня
		ab_today.account_out_sum AS account_out_sum
-- 	Текущий день
	FROM rd.account_balance AS ab_today
-- 	Предыдущий день
	LEFT JOIN rd.account_balance AS ab_prev_day
		ON ab_today.account_rk = ab_prev_day.account_rk
		AND	ab_today.effective_date = ab_prev_day.effective_date + INTERVAL '1 day'
	WHERE ab_today.account_in_sum <> ab_prev_day.account_out_sum
)
UPDATE rd.account_balance AS ab
SET account_in_sum = (
	SELECT account_in_sum
	FROM ab_today_prev_day AS ab2
	WHERE ab.account_rk = ab2.account_rk
		AND ab.effective_date = ab2.effective_date
)
WHERE (ab.account_rk, ab.effective_date) IN (
	SELECT account_rk, effective_date
	FROM ab_today_prev_day
)
ROLLBACK;

-- Выборка корректных значений account_out_sum
SELECT 
	ab_prev_day.account_rk,
	ab_prev_day.effective_date,
-- 	ab_prev_day.account_out_sum AS prev_day_account_out_sum,
	COALESCE(ab_today.account_in_sum, ab_prev_day.account_out_sum) AS account_out_sum,
	ab_prev_day.account_in_sum AS account_in_sum
FROM rd.account_balance AS ab_prev_day
LEFT JOIN rd.account_balance AS ab_today
	ON ab_today.account_rk = ab_prev_day.account_rk
	AND	ab_today.effective_date = ab_prev_day.effective_date + INTERVAL '1 day'
WHERE ab_today.account_in_sum <> ab_prev_day.account_out_sum

-- Исправление на корректные account_out_sum
BEGIN;
WITH ab_today_prev_day AS (
	SELECT 
-- 		Идентификатор счета
		ab_prev_day.account_rk,
-- 		Дата
		ab_prev_day.effective_date,
-- 		Сумма на конец дня
		COALESCE(ab_today.account_in_sum, ab_prev_day.account_out_sum) AS account_out_sum,
-- 		Сумма на начало дня
		ab_prev_day.account_in_sum AS account_in_sum
-- 	Предыдущий день
	FROM rd.account_balance AS ab_prev_day
-- 	Текущий день
	LEFT JOIN rd.account_balance AS ab_today
		ON ab_today.account_rk = ab_prev_day.account_rk
		AND	ab_today.effective_date = ab_prev_day.effective_date + INTERVAL '1 day'
	WHERE ab_today.account_in_sum <> ab_prev_day.account_out_sum
)
UPDATE rd.account_balance AS ab
SET account_out_sum = (
	SELECT account_out_sum
	FROM ab_today_prev_day AS ab2
	WHERE ab.account_rk = ab2.account_rk
		AND ab.effective_date = ab2.effective_date
)
WHERE (ab.account_rk, ab.effective_date) IN (
	SELECT account_rk, effective_date
	FROM ab_today_prev_day
)
ROLLBACK;

-- Проверка загрузки из csv-файла
SELECT * FROM dm.dict_currency

-- Удаление новых данных для проверок
DELETE FROM dm.dict_currency
WHERE currency_cd = '500'

-- Использование процедуры для заполнения dm.account_balance_turnover
BEGIN;
CALL fill_account_balance_turnover();
ROLLBACK;

SELECT * FROM dm.account_balance_turnover
WHERE currency_name = 'KZT';