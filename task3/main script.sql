DELETE FROM logs.log_table;

DELETE FROM dm.dm_f101_round_f;

-- Заполнение витрины dm_f101_round_f
SELECT * FROM dm.dm_f101_round_f;
CALL dm.fill_f101_round_f('2018-02-01'::DATE);
SELECT * FROM dm.dm_f101_round_f;

SELECT * FROM logs.log_table;