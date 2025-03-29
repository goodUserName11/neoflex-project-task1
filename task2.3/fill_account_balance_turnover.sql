-- Процедура для заполнения витрины dm.account_balance_turnover
CREATE OR REPLACE PROCEDURE fill_account_balance_turnover ()
LANGUAGE plpgsql
AS $$
BEGIN
--  Удаление старых записей
	DELETE FROM dm.account_balance_turnover;
	
--  Вставка новых записей
	INSERT INTO dm.account_balance_turnover
--  Использование прототипа
	SELECT a.account_rk,
		   COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
		   a.department_rk,
		   ab.effective_date,
		   ab.account_in_sum,
		   ab.account_out_sum
	FROM rd.account a
	LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
	LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd;
	
END; $$;