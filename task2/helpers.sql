-- Процедура для расчета витрины dm_account_turnover_f за каждый день января 2018 года
CREATE OR REPLACE PROCEDURE fill_account_turnover_f_for_dates(start_date date, end_date date)
LANGUAGE plpgsql
AS $$
DECLARE
	date_iterator date;
BEGIN
	date_iterator = start_date;
	
	WHILE date_iterator <= end_date LOOP
		CALL ds.fill_account_turnover_f(date_iterator);
        COMMIT;
		
		date_iterator = date_iterator + INTERVAL '1 day';
	END LOOP;
END; $$;

-- Процедура начального заполения витрины dm_account_balance_f
CREATE OR REPLACE PROCEDURE account_balance_f_init()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dm.dm_account_balance_f
	SELECT 
		b.on_date, 
		b.account_rk, 
		b.balance_out,
		(CASE
			WHEN er.reduced_cource IS NULL 
				THEN b.balance_out * 1
			ELSE b.balance_out * er.reduced_cource
		END)AS balance_out_rub
	FROM ds.ft_balance_f AS b
	LEFT JOIN ds.md_exchange_rate_d AS er
		ON b.currency_rk = er.currency_rk
			AND '2017-12-31'::DATE BETWEEN er.data_actual_date AND er.data_actual_end_date
	;
END; $$;

-- Процедура для расчета витрины dm_account_balance_f за каждый день января 2018 года
CREATE OR REPLACE PROCEDURE fill_account_balance_f_for_dates(start_date date, end_date date)
LANGUAGE plpgsql
AS $$
DECLARE
	date_iterator date;
BEGIN
	date_iterator = start_date;
	
	WHILE date_iterator <= end_date LOOP
		CALL ds.fill_account_balance_f(date_iterator);
        COMMIT;
		
		date_iterator = date_iterator + INTERVAL '1 day';
	END LOOP;
END; $$;