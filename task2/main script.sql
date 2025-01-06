DELETE FROM logs.log_table;

-- Заполнение витрины dm_account_turnover_f
DELETE FROM dm.dm_account_turnover_f;

SELECT * FROM dm.dm_account_turnover_f;
CALL fill_account_turnover_f_for_dates('2018.01.01'::DATE, '2018.01.31'::DATE);
SELECT * FROM dm.dm_account_turnover_f;

SELECT * FROM logs.log_table;

-- Заполнение витрины dm_account_turnover_f
DELETE FROM dm.dm_account_balance_f;
-- Начальное заполнение витрины dm_account_balance_f
CALL account_balance_f_init();

SELECT * FROM dm.dm_account_balance_f;
CALL fill_account_balance_f_for_dates('2018-01-01'::DATE, '2018-01-31'::DATE);
SELECT * FROM dm.dm_account_balance_f;

SELECT * FROM logs.log_table;