CREATE OR ALTER PROCEDURE bronze.cleanup_prices
AS
DELETE FROM bronze.prices
WHERE load_timestamp < DATEADD(day, -3, SYSUTCDATETIME());