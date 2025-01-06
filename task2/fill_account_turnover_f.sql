-- Процедура заполнения витрины оборотов по лицевым счетам
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate date)
LANGUAGE plpgsql
AS $$
DECLARE
	curr_log_id int;
	msg_txt text;
	exception_detail text;
BEGIN
	INSERT INTO logs.log_table(log_level, log_message)
	VALUES ('INFO', 'Processing of dm_account_turnover_f for date ' || i_OnDate)
	RETURNING log_id
	INTO curr_log_id;

	DELETE FROM dm.dm_account_turnover_f
	WHERE on_date = i_OnDate;
	
	WITH credit_debet_balance AS (
		SELECT 
			i_OnDate AS on_date,
			b.account_rk,
			SUM(pcredit.credit_amount) AS credit_amount,
			SUM(pdebet.debet_amount) AS debet_amount,
			er.reduced_cource
		FROM ds.ft_balance_f AS b
		LEFT JOIN ds.md_exchange_rate_d AS er
			ON b.currency_rk = er.currency_rk
				AND i_OnDate BETWEEN er.data_actual_date AND er.data_actual_end_date
		JOIN ds.ft_posting_f AS pcredit
			ON b.account_rk = pcredit.credit_account_rk
				AND pcredit.oper_date = i_OnDate
		JOIN ds.ft_posting_f AS pdebet
			ON b.account_rk = pdebet.debet_account_rk
				AND pdebet.oper_date = i_OnDate
		GROUP BY 
			b.account_rk,
			er.reduced_cource
	)
	INSERT INTO dm.dm_account_turnover_f
	SELECT 
		on_date,
		account_rk AS account_rk,
		credit_amount,
		(CASE
			WHEN reduced_cource IS NULL 
				THEN credit_amount * 1
			ELSE credit_amount * reduced_cource
		END) AS credit_amount_rub,
		debet_amount,
		(CASE
			WHEN reduced_cource IS NULL 
				THEN debet_amount * 1
			ELSE debet_amount * reduced_cource
		END) AS debet_amount_rub
	FROM credit_debet_balance;
	
	UPDATE logs.log_table
	SET end_timestamp = CURRENT_TIMESTAMP
	WHERE log_id = curr_log_id;
	
	EXCEPTION
		WHEN OTHERS THEN
			GET STACKED DIAGNOSTICS msg_txt = MESSAGE_TEXT,
									exception_detail = PG_EXCEPTION_DETAIL;
			
			INSERT INTO logs.log_table(log_level, log_message)
			VALUES (
				'ERROR', 
				'Error while Processing of dm_account_turnover_f for date ' 
				|| i_OnDate || ': ' || msg_txt || '. details: ' || exception_detail);

			RAISE;
END; $$;