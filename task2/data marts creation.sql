-- Создание схемы dm
CREATE SCHEMA IF NOT EXISTS dm;
-- Создание витрины оборотов по лицевым счетам
CREATE TABLE dm.dm_account_turnover_f(
	on_date DATE,
	account_rk NUMERIC,
	credit_amount NUMERIC(23,8),
	credit_amount_rub NUMERIC(23,8),
	debet_amount NUMERIC(23,8),
	debet_amount_rub NUMERIC(23,8)
)

-- Создание витрины остатков по лицевым счетам
CREATE TABLE dm.dm_account_balance_f(
	on_date DATE,
	account_rk NUMERIC,
	balance_out FLOAT,
	balance_out_rub FLOAT
)