/* ---
Laddar in referensdata för hushållsapparater.
--- */

IF NOT EXISTS (SELECT 1 FROM ref.appliances)
BEGIN
    INSERT INTO ref.appliances(appliance_name, kwh, usage_label)
    VALUES
        ('Golvvärme (6 m²)',    3.7, 'per dygn'),
        ('TV (LED 55 tum)',     0.1, 'per timme'),
        ('Handdukstork',        1.44, 'per dygn'),
        ('Dusch',               5.0, '10 min'),
        ('Bad',                 6.0, '1 bad'),
        ('Torktumlare',         6.0, '1 torkning'),
        ('Tvättmaskin',         1.0, '1 tvätt'),
        ('Diskmaskin',          1.0, '1 körning'),
        ('Kyl och frys',        0.548, 'per dygn'),
        ('Spisplatta',          1.750, 'per timme'),
        ('Ugn',                 1.000, 'per timme');
END;