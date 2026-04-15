/* -----------------
Denna koden skapar proceduren silver.load_silver som laddar in data
från bronslagret i silverlagret. Data transformeras och validering utförs.
*/ -----------------

CREATE OR ALTER PROCEDURE silver.load_silver 
        @full_refresh BIT = 0 -- Ifall man vill ladda all data på nytt får man göra en full refresh sätta värdet till 1 vid exekution. EXEC silver.load_silver @full_refresh = 1
        -- Parametrar deklareras före AS eftersom de är input till proceduren och definierar dess signatur (värden skickas in vid EXEC).
AS
SET XACT_ABORT, NOCOUNT ON -- XACT_ABORT instructs SQL Server to rollback the entire transaction and abort the batch when a run-time error occurs.

BEGIN TRY
        BEGIN TRANSACTION;

        DECLARE @rows_inserted INT;

        /* -------------------------------
        Validering
        ------------------------------- */
        DECLARE @validation NVARCHAR(MAX) = '';

        -- Kontrollerar zonkod
        IF EXISTS ( 
                SELECT 1
                FROM bronze.prices 
                WHERE SUBSTRING(source_file, 17, 3) NOT IN ('SE1','SE2','SE3','SE4')
        )
        SET @validation = @validation + 'Zone must be SE1, SE2, SE3 or SE4. ';

        -- Kontrollerar tid
        IF EXISTS ( 
                SELECT 1
                FROM bronze.prices  
                WHERE time_end <= time_start
        )
        SET @validation = @validation + 'time_start cannot begin after time_end. ';

        IF LEN(@validation) > 0
                THROW 50001, @validation, 1;

        /* --------------------------------
        Truncate vid full refresh
        -------------------------------- */
        IF @full_refresh = 1 
        TRUNCATE TABLE silver.prices;

        /* --------------------------------
        transformering och laddning 
        -------------------------------- */
        INSERT INTO silver.prices (
                sek_per_kwh,
                eur_per_kwh,
                exchange_rate,
                time_start,
                time_end,
                duration_minutes,
                zone_code,
                source_file
        )
        SELECT
                b.sek_per_kwh,
                b.eur_per_kwh,
                b.exr,
                b.time_start,
                b.time_end,
                duration_minutes = CASE
                        WHEN b.time_start < '2025-10-01' THEN 60
                        ELSE 15
                END,
                SUBSTRING(b.source_file, 17, 3),
                source_file
        FROM bronze.prices b  
        WHERE NOT EXISTS (
                SELECT 1
                FROM silver.prices s
                WHERE s.zone_code = SUBSTRING(b.source_file, 17, 3)
                AND s.time_start = b.time_start
        );

        SET @rows_inserted = @@ROWCOUNT;

        INSERT INTO silver.load_log (
                procedure_name,
                source_file,
                rows_inserted,
                status_type
        )
        VALUES (
                OBJECT_NAME(@@PROCID), -- loggar namn på procedur som körs
                (SELECT TOP 1 source_file FROM bronze.prices),
                @rows_inserted,
                'SUCCESS'
        );

        COMMIT TRANSACTION;

END TRY
BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

        INSERT INTO silver.load_log (
                procedure_name,
                source_file,
                rows_inserted,
                status_type,
                error_message
        )
        VALUES (
                OBJECT_NAME(@@PROCID), -- loggar namn på procedur som körs
                (SELECT TOP 1 source_file FROM bronze.prices),
                0,
                'FAIL',
                ERROR_MESSAGE()
        );

        ;THROW -- Semikolon före Throw brukar användas för att minska risken att råka
        -- göra THROW del av föregående argument om det föregående saknar ;, vilket kan skapa dolda buggar.
END CATCH;
