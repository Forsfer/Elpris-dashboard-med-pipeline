/* ---
Denna koden skapar tabellerna i guldschemat som tillsammans är ett star schema.
*/ ---

IF OBJECT_ID('gold.dim_zone', 'U') IS NULL
BEGIN
	CREATE TABLE gold.dim_zone (
        zone_key  INT identity(1,1) PRIMARY KEY,
	    zone_code VARCHAR(10) NOT NULL,
            CONSTRAINT CK_gold_dim_zone_zone_code
        	CHECK (zone_code IN ('SE1','SE2','SE3','SE4')),
        zone_name VARCHAR(30) NOT NULL,
            CONSTRAINT CK_gold_dim_zone_zone_name
        	CHECK (zone_name IN ('Luleå','Sundsvall','Stockholm','Malmö')),
	);
END;

GO

IF OBJECT_ID('gold.dim_date', 'U') IS NULL
BEGIN
	CREATE TABLE gold.dim_date (
        date_key     INT identity(1,1) PRIMARY KEY,
	    year         SMALLINT NOT NULL,
        quarter      TINYINT NOT NULL,
        month        TINYINT NOT NULL,
        month_name   VARCHAR(10) NOT NULL,
            CONSTRAINT CK_gold_dim_date_month_name
        	CHECK (month_name IN ('januari','februari','mars','april','maj','juni','juli','augusti','september','oktober','november','december')),
        date         DATE NOT NULL,
        day_name VARCHAR(8) NOT NULL,
            CONSTRAINT CK_gold_dim_date_day_name
        	CHECK (day_name IN ('måndag','tisdag','onsdag','torsdag','fredag','lördag','söndag')),
        day_of_week  TINYINT NOT NULL,
        day_of_month TINYINT NOT NULL
	);
END;

GO

IF OBJECT_ID('gold.dim_time', 'U') IS NULL
BEGIN
	CREATE TABLE gold.dim_time (
        time_key     INT identity(1,1) PRIMARY KEY,
	    hour         TINYINT NOT NULL, /* t.ex. 23 */
        quarter_hour TINYINT NOT NULL, /*1, 2, 3, 4. Vilken kvart i timmen det handlar om. Är det timpris blir det 1*/
        time_label   CHAR(5) NOT NULL /* t.ex. 20:00 */
	);
END;

GO

IF OBJECT_ID('gold.fact_prices', 'U') IS NULL
BEGIN
	CREATE TABLE gold.fact_prices (
	    zone_key         INT NOT NULL,
        date_key         INT NOT NULL,
        time_key         INT NOT NULL,
        
        sek_per_kwh      DECIMAL(10,5) NOT NULL,
	    eur_per_kwh      DECIMAL(10,5) NOT NULL,
	    exchange_rate    DECIMAL(12,6) NOT NULL,
        duration_minutes TINYINT NOT NULL,
        CONSTRAINT CK_gold_fact_prices_duration_minutes
        	CHECK (duration_minutes IN (15, 60)),

        timestamp_utc    DATETIME2, /* UTC har inte vinter/sommartid, så den kan användas för det problemet */

        CONSTRAINT PK_fact_prices
        PRIMARY KEY (zone_key, date_key, time_key, timestamp_utc),
        
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

GO