CREATE OR ALTER PROCEDURE bronze.delete_old_prices_bronze
AS
DELETE FROM bronze.prices
WHERE load_timestamp < DATEADD(day, -3, SYSUTCDATETIME());