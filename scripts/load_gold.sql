/* -----------------
Denna koden skapar proceduren gold.load_gold som laddar in data
från silverlagret till guldlagret. Data laddas in i star schema-tabellerna i guldlagret och data berikas.
*/ -----------------

CREATE OR ALTER PROCEDURE gold.load_gold AS
SET XACT_ABORT, NOCOUNT ON -- XACT_ABORT instructs SQL Server to rollback the entire transaction and abort the batch when a run-time error occurs.
SET LANGUAGE Swedish; -- Detta görs för att DATE-funktionerna ska ge månader och dagsnamn på svenska.
SET DATEFIRST 1; -- Veckan börjas på måndag så day_of_week = 1–7 (måndag–söndag). Istället för att tisdag är dag 1 i en specifik vecka.

BEGIN TRY
        BEGIN TRANSACTION;

        /* ******************************
        Transformering och laddning 
        ****************************** */

        -------------- dim_zone-tabell --------------
        INSERT INTO gold.dim_zone (
                zone_code,
                zone_name
        )
        SELECT DISTINCT -- DISTINCT används för att hämta bara unika värden (t.ex. en rad per zon eller datum),
                        -- eftersom samma värde kan förekomma många gånger.
                s.zone_code,
                zone_name =
                CASE
                    WHEN s.zone_code = 'SE1' then 'Luleå'
                    WHEN s.zone_code = 'SE2' then 'Sundsvall'
                    WHEN s.zone_code = 'SE3' then 'Stockholm'
                    ELSE 'Malmö'
                END
   
        FROM silver.prices s
        WHERE NOT EXISTS (
                SELECT 1
                FROM gold.dim_zone g
                WHERE g.zone_code = s.zone_code
        );


        -------------- dim_date-tabell --------------
        INSERT INTO gold.dim_date (
                year,
                quarter,
                month,
                month_name,
                date,
                day_name,
                day_of_week,
                day_of_month
        )
        SELECT DISTINCT
                YEAR(s.time_start) AS year,
                DATEPART(QUARTER, s.time_start) AS quarter, -- Ger kvartal i form av entalig siffra
                DATEPART(MONTH, s.time_start) AS month, -- Ger månadsnummer
                DATENAME(MONTH, s.time_start) AS month_name, -- Ger månadsnamn
                CONVERT(DATE, s.time_start) AS date, -- 2026-03-06
                DATENAME(WEEKDAY, s.time_start) AS day_name, -- Ger veckodagsnamn
                DATEPART(WEEKDAY, s.time_start) AS day_of_week, -- Veckodagsnummer 
                DAY(s.time_start) AS day_of_month -- Dagens nummer i månaden
        FROM silver.prices s
        WHERE NOT EXISTS (
                SELECT 1
                FROM gold.dim_date d
                WHERE d.date = CONVERT(date, s.time_start)
        );


        -------------- dim_time-tabell --------------
        INSERT INTO gold.dim_time (
                hour,
                quarter_hour,
                time_label
        )
        SELECT DISTINCT
            DATEPART(HOUR, s.time_start) AS hour,
            DATEPART(MINUTE, s.time_start) / 15 + 1 AS quarter_hour, -- '/ 15' ger kvarter. +1 för att börja på timmen på 1 istället för 0.
            FORMAT(s.time_start, 'HH:mm') AS time_label
   
        FROM silver.prices s
        WHERE NOT EXISTS (
                SELECT 1
                FROM gold.dim_time g
                WHERE g.hour = DATEPART(HOUR, s.time_start) -- hour och quarter_hour så att det fångar upp 20:15 och inte allt under kl 20.
                AND g.quarter_hour = DATEPART(MINUTE, s.time_start)/15 + 1
        );

        -------------|- fact_prices-tabell -|-------------
        DECLARE @rows_inserted INT; -- Håller koll på antal rader som laddas in. Varje fact-rad är unik så man loggar inte rader för dimensioner.

        INSERT INTO gold.fact_prices (
                zone_key,
                date_key,
                time_key,
                sek_per_kwh,
                eur_per_kwh,
                exchange_rate,
                duration_minutes,
                timestamp_utc
        )
        SELECT -- Inte distinct här som i dimension, eftersom varje rad är unik
                z.zone_key AS zone_key,
                d.date_key AS date_key,
                t.time_key AS time_key,

                s.sek_per_kwh AS sek_per_kwh,
                s.eur_per_kwh AS eur_per_kwh,
                s.exchange_rate AS exchange_rate,
                s.duration_minutes AS duration_minutes,
                CAST(s.time_start AT TIME ZONE 'UTC' AS datetime2) AS timestamp_utc
   
        FROM silver.prices s

        JOIN gold.dim_zone z
            ON z.zone_code = s.zone_code

        JOIN gold.dim_date d
            ON d.date = CONVERT(date, s.time_start)

        JOIN gold.dim_time t
                ON  t.hour         = DATEPART(HOUR,   SWITCHOFFSET(s.time_start, '+00:00'))
                AND t.quarter_hour = DATEPART(MINUTE, SWITCHOFFSET(s.time_start, '+00:00'))/15 + 1


        WHERE NOT EXISTS (
                SELECT 1
                FROM gold.fact_prices f
                WHERE f.zone_key = z.zone_key
                AND f.timestamp_utc = CAST(s.time_start AT TIME ZONE 'UTC' AS datetime2)
        )

        /* ******************************
                Loggning 
        ****************************** */        
        SET @rows_inserted = @@ROWCOUNT;

        INSERT INTO gold.load_log (
                procedure_name,
                rows_inserted,
                status_type
        )
        VALUES (
                OBJECT_NAME(@@PROCID), -- loggar namn på procedur som körs
                @rows_inserted,
                'SUCCESS'
        );

        COMMIT TRANSACTION;

END TRY
BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

        INSERT INTO gold.load_log (
                procedure_name,
                rows_inserted,
                status_type,
                error_message
        )
        VALUES (
                OBJECT_NAME(@@PROCID), -- loggar namn på procedur som körs
                0,
                'FAIL',
                ERROR_MESSAGE()
        );

        ;THROW -- Semikolon före Throw brukar användas för att minska risken att råka
        -- göra THROW del av föregående argument om det föregående saknar ;, vilket kan skapa dolda buggar.
END CATCH;
