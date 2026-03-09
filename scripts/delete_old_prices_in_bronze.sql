CREATE OR ALTER PROCEDURE bronze.delete_old_prices_bronze
AS
DELETE FROM bronze.prices
WHERE time_start < DATEADD(day, -3, SYSUTCDATETIME());