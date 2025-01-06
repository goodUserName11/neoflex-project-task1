-- Процедура расчета 101 формы 
CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate date)
LANGUAGE plpgsql
AS $$
DECLARE
	curr_log_id int;
	msg_txt text;
	exception_detail text;
BEGIN
-- 	Сначало происходит логирование начала операции
	INSERT INTO logs.log_table(log_level, log_message)
	VALUES ('INFO', 'Processing of dm_f101_round_f for date ' || i_OnDate)
	RETURNING log_id
	INTO curr_log_id;
	
-- 	Удаление записей за отчетный период, для перерасчета
	DELETE FROM dm.dm_f101_round_f
	WHERE "TO_DATE" = i_OnDate - INTERVAL '1 day';

--	С помощью cte собираем необходимые столбцы для расчета
-- 	cte для получения дат для отчетного периода
	WITH start_date_to_date AS (
		SELECT
			i_OnDate - INTERVAL '1 month' AS "FROM_DATE",
			i_OnDate - INTERVAL '1 day' AS "TO_DATE",
			i_OnDate - INTERVAL '1 month 1 day' AS before_from_date
	), 
--	cte для получения сумм остатков в рублях за день, предшествующий первому дню отчетного периода 
	balance_in AS (
		SELECT
			SUBSTRING(a.account_number FROM 1 FOR 5) AS ledger_account, 
			SUM (CASE 
				WHEN a.currency_code = '810' 
					OR a.currency_code = '643'
					THEN ab.balance_out_rub
				ELSE 0
			END) AS "BALANCE_IN_RUB",
			SUM (CASE 
				WHEN a.currency_code <> '810' 
					AND a.currency_code <> '643'
					THEN ab.balance_out_rub
				ELSE 0
			END) AS "BALANCE_IN_VAL",
			SUM (ab.balance_out_rub) AS "BALANCE_IN_TOTAL"
		FROM dm.dm_account_balance_f AS ab
		JOIN ds.md_account_d AS a
			ON a.account_rk = ab.account_rk
		JOIN start_date_to_date AS sdtd
			ON 1 = 1
		WHERE ab.on_date = sdtd.before_from_date
		GROUP BY ledger_account
	),
--	cte для получения сумм дебетовых и кредитовых оборотов в рублях за все дни отчетного периода 
	turnover_sum AS (
		SELECT
			SUBSTRING(a.account_number FROM 1 FOR 5) AS ledger_account,
			SUM (CASE
				WHEN a.currency_code = '810' 
					OR a.currency_code = '643'
				 	THEN debet_amount_rub
				ELSE 0
			END) AS "TURN_DEB_RUB",
			SUM (CASE
				WHEN a.currency_code <> '810' 
					AND a.currency_code <> '643'
				 	THEN debet_amount_rub
				ELSE 0
			END) AS "TURN_DEB_VAL",
			SUM (debet_amount_rub) AS "TURN_DEB_TOTAL",
			SUM (CASE
				WHEN a.currency_code = '810' 
					OR a.currency_code = '643'
				 	THEN credit_amount_rub
				ELSE 0
			END) AS "TURN_CRE_RUB",
			SUM (CASE
				WHEN a.currency_code <> '810' 
					AND a.currency_code <> '643'
				 	THEN credit_amount_rub
				ELSE 0
			END) AS "TURN_CRE_VAL",
			SUM (credit_amount_rub) AS "TURN_CRE_TOTAL"
		FROM dm.dm_account_turnover_f AS at
		JOIN ds.md_account_d AS a
			ON a.account_rk = at.account_rk
		JOIN start_date_to_date AS sdtd
			ON 1 = 1
		WHERE at.on_date BETWEEN sdtd."FROM_DATE" AND sdtd."TO_DATE"
		GROUP BY ledger_account
	),
--	cte для получения сумм остатков в рублях за последний день отчетного периода 
	balance_out AS (
		SELECT
			SUBSTRING(a.account_number FROM 1 FOR 5) AS ledger_account, 
			SUM (CASE 
				WHEN a.currency_code = '810' 
					OR a.currency_code = '643'
					THEN ab.balance_out_rub
				ELSE 0
			END) AS "BALANCE_OUT_RUB",
			SUM (CASE 
				WHEN a.currency_code <> '810' 
					AND a.currency_code <> '643'
					THEN ab.balance_out_rub
				ELSE 0
			END) AS "BALANCE_OUT_VAL",
			SUM (ab.balance_out_rub) AS "BALANCE_OUT_TOTAL"
		FROM dm.dm_account_balance_f AS ab
		JOIN ds.md_account_d AS a
			ON a.account_rk = ab.account_rk
		JOIN start_date_to_date AS sdtd
			ON 1 = 1
		WHERE ab.on_date = sdtd."TO_DATE"
		GROUP BY ledger_account
	),
--	cte для получения информации о счетах сгрупированных по балансовым счетам второго порядка 
	account_data AS (
		SELECT
			sdtd."FROM_DATE",
			sdtd."TO_DATE",
			la.chapter AS "CHAPTER",
			la.ledger_account::CHAR(5) AS "LEDGER_ACCOUNT",
			a.char_type AS "CHARACTERISTIC"
		FROM ds.md_account_d AS a
		JOIN ds.md_ledger_account_s AS la
			ON SUBSTRING(a.account_number FROM 1 FOR 5) = la.ledger_account::CHAR(5)
		JOIN start_date_to_date AS sdtd
			ON 1 = 1
		GROUP BY 
			"LEDGER_ACCOUNT",
			"FROM_DATE",
			"TO_DATE",
			"CHAPTER",
			"CHARACTERISTIC"
	)
-- 	Вставка в витрину dm_f101_round_f
-- в поля с префиксом r_ неописано, что встявлять 
	INSERT INTO dm.dm_f101_round_f ("FROM_DATE", "TO_DATE", "CHAPTER", "LEDGER_ACCOUNT", 
									"CHARACTERISTIC", "BALANCE_IN_RUB", "BALANCE_IN_VAL", 
								   "BALANCE_IN_TOTAL", "TURN_DEB_RUB", "TURN_DEB_VAL", 
									"TURN_DEB_TOTAL", "TURN_CRE_RUB", "TURN_CRE_VAL", 
								   "TURN_CRE_TOTAL", "BALANCE_OUT_RUB", "BALANCE_OUT_VAL",
								   "BALANCE_OUT_TOTAL")
-- 	Сбор столбцов, которые будут вставлены
	SELECT
		ad."FROM_DATE",
		ad."TO_DATE",
		ad."CHAPTER",
		ad."LEDGER_ACCOUNT",
		ad."CHARACTERISTIC",
		bi."BALANCE_IN_RUB",
		bi."BALANCE_IN_VAL",
		bi."BALANCE_IN_TOTAL",
		COALESCE(ts."TURN_DEB_RUB", 0) AS "TURN_DEB_RUB",
		COALESCE(ts."TURN_DEB_VAL", 0) AS "TURN_DEB_VAL",
		COALESCE(ts."TURN_DEB_TOTAL", 0) AS "TURN_DEB_TOTAL",
		COALESCE(ts."TURN_CRE_RUB", 0) AS "TURN_CRE_RUB",
		COALESCE(ts."TURN_CRE_VAL", 0) AS "TURN_CRE_VAL",
		COALESCE(ts."TURN_CRE_TOTAL", 0)AS "TURN_CRE_TOTAL",
		bo."BALANCE_OUT_RUB",
		bo."BALANCE_OUT_VAL",
		bo."BALANCE_OUT_TOTAL"
	FROM account_data AS ad
	JOIN balance_in AS bi
		ON bi.ledger_account = ad."LEDGER_ACCOUNT"
	LEFT JOIN turnover_sum AS ts
		ON ts.ledger_account = ad."LEDGER_ACCOUNT"
	JOIN balance_out AS bo
		ON bo.ledger_account = ad."LEDGER_ACCOUNT"
	;
	
-- 	Запись конца операции
	UPDATE logs.log_table
	SET end_timestamp = CURRENT_TIMESTAMP
	WHERE log_id = curr_log_id;

-- 	Логирование ошибки
	EXCEPTION
		WHEN OTHERS THEN
			GET STACKED DIAGNOSTICS msg_txt = MESSAGE_TEXT,
									exception_detail = PG_EXCEPTION_DETAIL;
			
			INSERT INTO logs.log_table(log_level, log_message)
			VALUES (
				'ERROR', 
				'Error while Processing dm_f101_round_f for date ' 
				|| i_OnDate || ': ' || msg_txt || '. details: ' || exception_detail);

			RAISE;
END; $$;