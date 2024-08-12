SELECT * FROM etl.world_bank_data;
SELECT * FROM etl.exchanges_rates;
SELECT * FROM etl.process_logs;


TRUNCATE TABLE etl.world_bank_data RESTART IDENTITY;
TRUNCATE TABLE etl.exchanges_rates RESTART IDENTITY;