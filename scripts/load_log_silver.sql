/* ---
Denna koden skapar tabellen load_log i silverschemat som ska matas av data från proceduren load_silver som transformerar och laddar 
in data i silver.prices.
*/ ---

IF OBJECT_ID('silver.load_log', 'U') IS NULL
BEGIN
	CREATE TABLE silver.load_log (
	    load_id         INT identity(1,1) primary key,
		procedure_name  VARCHAR(100),
        source_file     VARCHAR(260) NULL,
        rows_inserted   INT NULL,
        status_type     VARCHAR(20) NULL, /* kan innehålla värden som SUCCESS, FAILED, RETRY, PARTIAL_SUCCESS osv*/
	    error_message   VARCHAR(500) NULL,
	    proc_timestamp  DATETIME2 NOT NULL DEFAULT sysdatetime()
	);
END;