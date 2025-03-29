-- В данных в файле есть только записи, которых нет в таблице
SELECT effective_from_date, effective_to_date, COUNT(1) FROM RD.deal_info
GROUP BY effective_from_date, effective_to_date

-- Удаление новых данных для проверок
-- DELETE FROM RD.deal_info
-- WHERE effective_from_date = '2023-03-15'::DATE

-- В данных в файле есть данные за даты, которые уже есть в таблице. Количество данных за одинаковые даты совпадает совпадает
SELECT effective_from_date, effective_to_date, COUNT(1) FROM RD.product
GROUP BY effective_from_date, effective_to_date

-- Удаление новых данных для проверок
-- DELETE FROM RD.product
-- WHERE effective_from_date <> '2023-03-15'

-- Заполнение витрины dm.loan_holiday_info
BEGIN;
CALL fill_loan_holiday_info();
END;

SELECT * FROM dm.loan_holiday_info;