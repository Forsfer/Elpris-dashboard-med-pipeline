/* ---
Denna koden skapar tabellen prices i bronsschemat, tänkt att lagra rå elprisdata som hämtas från API och laddas in från data lake.
Tabellen speglar strukturen i JSON-datan som hämtas in. Utöver källdatan innehåller tabellen surrogatnyckel, tidsstämpel för inladdning och namn på källfil. 
*/ ---

IF OBJECT_ID('bronze.prices', 'U') IS NULL
BEGIN
	CREATE TABLE bronze.prices (
		raw_id bigint identity(1,1) primary key, /* Detta kunde nog varit vanlig INT istället */
		
		/* I SQL-server behöver man egentligen inte skriva NULL eftersom det är standardvärde, men det är bra praxis utifall miljöbyte
		skulle ske. */
	    sek_per_kwh     decimal(10,5) NULL,
	    eur_per_kwh     decimal(10,5) NULL,
	    exr             decimal(12,6) NULL,
	    time_start      datetimeoffset NULL,
	    time_end        datetimeoffset NULL,
	
	    load_timestamp datetime2 NOT NULL DEFAULT sysdatetime(),
	    source_file     varchar(260) NULL, /* Namn på källfilen */

		CONSTRAINT UQ_bronze_file_time -- Fixar problemet att det blir en dubbel inladdning av fil
        	UNIQUE (source_file, time_start)
	);
END;