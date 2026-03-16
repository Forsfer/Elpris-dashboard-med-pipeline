/* ---
Denna koden skapar tabellen ref.appliances som innehåller referensdata för hushållsapparaters elförbrukning. 
Den används för att beräkna kostnad per användningstillfälle baserat på aktuellt spotpris.
--- */

IF OBJECT_ID('ref.appliances', 'U') IS NULL
BEGIN
    CREATE TABLE ref.appliances (
        appliance_key   int             IDENTITY(1,1)   PRIMARY KEY,
        appliance_name  nvarchar(100)   NOT NULL,
        kwh             decimal(6,3)    NOT NULL,
        usage_label     nvarchar(50)    NOT NULL        -- Beskriver vad kWh-värdet avser, t.ex. "1 tvätt" eller "per timme"
    );
END;