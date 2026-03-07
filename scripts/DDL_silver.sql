/* ---
Denna koden skapar tabellen prices i silverschemat, tänkt att lagra data som har processats genom proceduren load_silver.sql.
Duration_minutes är hur länge ett pris pågår, om det är 15 minuter eller 60 minuter.
*/ ---

IF OBJECT_ID('silver.prices', 'U') IS NULL
BEGIN
	CREATE TABLE silver.prices (
	    sek_per_kwh     decimal(10,5) NOT NULL,
	    eur_per_kwh     decimal(10,5) NOT NULL,
	    exchange_rate   decimal(12,6) NOT NULL,
	    time_start      datetimeoffset NOT NULL,
	    time_end        datetimeoffset NOT NULL, 
        duration_minutes TINYINT NOT NULL,
			CONSTRAINT CK_silver_prices_duration_minutes
        	CHECK (duration_minutes IN (15,60)),
        zone_code       varchar(10) NOT NULL,
			CONSTRAINT CK_silver_prices_zone_code
        	CHECK (zone_code IN ('SE1','SE2','SE3','SE4')),

	    proc_timestamp datetime2 NOT NULL DEFAULT sysdatetime(),
        CONSTRAINT PK_silver_prices
        PRIMARY KEY (zone_code, time_start)
	);
END;