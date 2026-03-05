/* ---
Denna koden skapar tabellerna i guldschemat som tillsammans är ett star schema.
*/ ---

IF OBJECT_ID('gold.dim_zone', 'U') IS NULL
BEGIN
	CREATE TABLE gold.dim_zone (
        zone_key  INT identity(1,1) PRIMARY KEY,
	    zone_code VARCHAR(10) NOT NULL,
        zone_name VARCHAR(30) NOT NULL 
	);
END;

IF OBJECT_ID('gold.dim_date', 'U') IS NULL
BEGIN
	CREATE TABLE gold.dim_date (
        date_key     INT identity(1,1) PRIMARY KEY,
	    year         SMALLINT NOT NULL,
        quarter      SMALLINT NOT NULL,
        month        SMALLINT NOT NULL,
        month_name   VARCHAR(15) NOT NULL,
        date         DATE NOT NULL,
        day_name_swe VARCHAR(8) NOT NULL,
        day_of_week  TINYINT NOT NULL,
        day_of_month TINYINT NOT NULL
	);
END;

IF OBJECT_ID('gold.dim_time', 'U') IS NULL
BEGIN
	CREATE TABLE gold.dim_time (
        time_key     INT identity(1,1) PRIMARY KEY,
	    hour         TINYINT NOT NULL, /* t.ex. 23 */
             /*Borde quarter_hour vara 1,2,3,4 istället? Enklare att visa i graf om det skulle behövas */
        quarter_hour TINYINT NOT NULL, /* 0, 1, 2, 3. Vilken kvart i timmen det handlar om. Är det timpris blir det 0*/
        time_label   CHAR(5) NOT NULL /* t.ex. 20:00 */
	);
END;


IF OBJECT_ID('gold.fact_prices', 'U') IS NULL
BEGIN
	CREATE TABLE gold.fact_prices (
	    sek_per_kwh      DECIMAL(10,5) NOT NULL,
	    eur_per_kwh      DECIMAL(10,5) NOT NULL,
	    exchange_rate    DECIMAL(12,6) NOT NULL,
        duration_minutes TINYINT NOT NULL,

        timestamp_utc    DATETIME2, /* UTC har inte vinter/sommartid, så den kan användas för det problemet */

        CONSTRAINT PK_fact_prices
        PRIMARY KEY (zone_key, date_key, time_key),
        
        CONSTRAINT FK_zone_key
            FOREIGN KEY (zone_key)
            REFERENCES gold.dim_zone(zone_key),
        
        CONSTRAINT FK_date_key
            FOREIGN KEY (date_key)
            REFERENCES gold.dim_date(date_key),

        CONSTRAINT FK_time_key
            FOREIGN KEY (time_key)
            REFERENCES gold.dim_time(time_key)
	);
END;