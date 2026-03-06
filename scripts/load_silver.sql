/* -----------------
Denna koden skapar proceduren silver.load_silver som laddar in data
från bronslagret i silverlagret. Data transformeras och validering utförs.
*/ -----------------

CREATE OR ALTER PROCEDURE silver.load_silver AS
SET XACT_ABORT, NOCOUNT ON -- XACT_ABORT instructs SQL Server to rollback the entire transaction and abort the batch when a run-time error occurs.

BEGIN TRY
        BEGIN TRANSACTION;

        DECLARE @rows_inserted INT;

        /* -------------------------------
        Validering
        ------------------------------- */
        DECLARE @validation NVARCHAR(MAX) = '';

        -- Kontrollerar duration
        IF EXISTS ( 
                SELECT 1
                FROM bronze.prices 
                WHERE DATEDIFF(minute, time_start, time_end) NOT IN (15,60)
        )
        SET @validation = @validation + 'duration_minutes must be either 15 or 60. ';

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
        Transformering och laddning 
        -------------------------------- */
        INSERT INTO silver.prices (
                sek_per_kwh,
                eur_per_kwh,
                exr,
                time_start,
                time_end,
                duration_minutes,
                zone_code
        )
        SELECT
                b.sek_per_kwh,
                b.eur_per_kwh,
                b.exr,
                b.time_start,
                b.time_end,
                DATEDIFF(minute, b.time_start, b.time_end),
                SUBSTRING(b.source_file, 17, 3)
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
