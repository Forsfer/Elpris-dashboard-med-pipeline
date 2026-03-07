/* ---
Denna koden skapar tabellen load_log i guldschemat som ska matas av data från proceduren load_gold som transformerar och laddar 
in data i de olika tabellerna i guldschemat: dim_zone, dim_time, dim_date, fact_prices.

ATT FÖRBÄTTRA: status_type kanske borde ha fasta värden man endast får välja?
*/ ---

IF OBJECT_ID('gold.load_log', 'U') IS NULL
BEGIN
	CREATE TABLE gold.load_log (
	    load_id         INT identity(1,1) primary key,
        procedure_name  VARCHAR(100), -- Proceduren som laddar in data
        rows_inserted   INT NULL,
        status_type     VARCHAR(20) NULL, /* Skulle kunna innehålla värden som SUCCESS, FAIL, RETRY, PARTIAL_SUCCESS osv*/
	    error_message   VARCHAR(500) NULL,
	    proc_timestamp  DATETIME2 NOT NULL DEFAULT sysdatetime()
	);
END;