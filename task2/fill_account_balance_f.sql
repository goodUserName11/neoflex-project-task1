-- Процедура заполнения витрины остатков по лицевым счетам dm_account_balance_f
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
	curr_log_id int;
	msg_txt text;
	exception_detail text;
BEGIN
	INSERT INTO logs.log_table(log_level, log_message)
	VALUES ('INFO', 'Processing of dm_account_balance_f for date ' || i_OnDate)
	RETURNING log_id
	INTO curr_log_id;
	
	DELETE FROM dm.dm_account_balance_f
	WHERE on_date = i_OnDate;

	WITH previous_day_account_data AS (
		SELECT
			i_OnDate AS on_date,
			a.account_rk,
			COALESCE(ab.balance_out, 0) AS balance_out,
			COALESCE(ab.balance_out_rub, 0) AS balance_out_rub,
			a.char_type,
			COALESCE(at.credit_amount, 0) AS credit_amount,
			COALESCE(at.debet_amount, 0) AS debet_amount,
			COALESCE(at.credit_amount_rub, 0) AS credit_amount_rub,
			COALESCE(at.debet_amount_rub, 0) AS debet_amount_rub
		FROM dm.dm_account_balance_f AS ab
		RIGHT JOIN ds.md_account_d AS a
			ON ab.account_rk = a.account_rk
				AND i_OnDate BETWEEN a.data_actual_date AND a.data_actual_end_date
				AND ab.on_date = i_OnDate - INTERVAL '1 day'
		LEFT JOIN dm.dm_account_turnover_f AS at
			ON a.account_rk = at.account_rk
				AND at.on_date = i_OnDate /*- INTERVAL '1 day' /*прошлый или текущий день? на конец дня остаток или на начало?*/*/
	)
	INSERT INTO dm.dm_account_balance_f
	SELECT 
		on_date,
		account_rk,
		(CASE
			WHEN char_type = 'А'
		 		THEN balance_out + debet_amount - credit_amount
		 	WHEN char_type = 'П'
		 		THEN balance_out - debet_amount + credit_amount
		END) AS balance_out,
		(CASE
			WHEN char_type = 'А'
		 		THEN balance_out_rub + debet_amount_rub - credit_amount_rub
		 	WHEN char_type = 'П'
		 		THEN balance_out_rub - debet_amount_rub + credit_amount_rub
		END) balance_out_rub
	FROM previous_day_account_data;
	
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
				'Error while Processing of dm_account_balance_f for date ' 
				|| i_OnDate || ': ' || msg_txt || '. details: ' || exception_detail);

			RAISE;
END; $$;
