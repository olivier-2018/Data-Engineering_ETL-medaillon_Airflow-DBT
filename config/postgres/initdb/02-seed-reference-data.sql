-- Static/lookup data for the `reference` schema (§2a of the redesign plan).
-- Not event-sourced (no Kafka topic feeds these), not a generator
-- hyperparameter (config.yaml holds rates/durations/counts only) - this is
-- structural reference data, seeded once here and editable afterward via
-- plain SQL (e.g. toggling reference.delivery_zones.is_active).

-- --- Delivery zones: 40 Swiss cities, first 10 active by default --------
-- Biel/Bienne doubles as the warehouse origin (is_warehouse=true) - this
-- replaces the old config.yaml warehouse.origin_lat/lon key entirely.
INSERT INTO reference.delivery_zones (city, canton, country, lat, lon, is_warehouse, is_active) VALUES
    ('Zürich',            'ZH', 'CH', 47.3769, 8.5417,  false, true),
    ('Geneva',            'GE', 'CH', 46.2044, 6.1432,  false, true),
    ('Basel',             'BS', 'CH', 47.5596, 7.5886,  false, true),
    ('Lausanne',          'VD', 'CH', 46.5197, 6.6323,  false, true),
    ('Bern',              'BE', 'CH', 46.9480, 7.4474,  false, true),
    ('Winterthur',        'ZH', 'CH', 47.5000, 8.7500,  false, true),
    ('Lucerne',           'LU', 'CH', 47.0502, 8.3093,  false, true),
    ('St. Gallen',        'SG', 'CH', 47.4245, 9.3767,  false, true),
    ('Lugano',            'TI', 'CH', 46.0037, 8.9511,  false, true),
    ('Biel/Bienne',       'BE', 'CH', 47.1368, 7.2468,  true,  true),
    ('Thun',              'BE', 'CH', 46.7580, 7.6280,  false, false),
    ('Köniz',             'BE', 'CH', 46.9241, 7.4139,  false, false),
    ('La Chaux-de-Fonds', 'NE', 'CH', 47.0999, 6.8252,  false, false),
    ('Fribourg',          'FR', 'CH', 46.8065, 7.1619,  false, false),
    ('Schaffhausen',      'SH', 'CH', 47.6970, 8.6350,  false, false),
    ('Chur',              'GR', 'CH', 46.8499, 9.5330,  false, false),
    ('Vernier',           'GE', 'CH', 46.2166, 6.0833,  false, false),
    ('Neuchâtel',         'NE', 'CH', 46.9900, 6.9293,  false, false),
    ('Uster',             'ZH', 'CH', 47.3474, 8.7208,  false, false),
    ('Sion',              'VS', 'CH', 46.2331, 7.3606,  false, false),
    ('Emmen',             'LU', 'CH', 47.0807, 8.3040,  false, false),
    ('Zug',               'ZG', 'CH', 47.1662, 8.5155,  false, false),
    ('Yverdon-les-Bains', 'VD', 'CH', 46.7785, 6.6413,  false, false),
    ('Kriens',            'LU', 'CH', 47.0333, 8.2833,  false, false),
    ('Rapperswil-Jona',   'SG', 'CH', 47.2267, 8.8180,  false, false),
    ('Dübendorf',         'ZH', 'CH', 47.3979, 8.6178,  false, false),
    ('Montreux',          'VD', 'CH', 46.4312, 6.9106,  false, false),
    ('Dietikon',          'ZH', 'CH', 47.4013, 8.4004,  false, false),
    ('Frauenfeld',        'TG', 'CH', 47.5590, 8.8990,  false, false),
    ('Wetzikon',          'ZH', 'CH', 47.3228, 8.7972,  false, false),
    ('Baar',              'ZG', 'CH', 47.1957, 8.5296,  false, false),
    ('Bulle',             'FR', 'CH', 46.6194, 7.0578,  false, false),
    ('Wil',               'SG', 'CH', 47.4623, 9.0442,  false, false),
    ('Renens',            'VD', 'CH', 46.5375, 6.5883,  false, false),
    ('Nyon',              'VD', 'CH', 46.3833, 6.2333,  false, false),
    ('Kreuzlingen',       'TG', 'CH', 47.6486, 9.1746,  false, false),
    ('Carouge',           'GE', 'CH', 46.1817, 6.1381,  false, false),
    ('Aarau',             'AG', 'CH', 47.3925, 8.0442,  false, false),
    ('Solothurn',         'SO', 'CH', 47.2088, 7.5323,  false, false),
    ('Bellinzona',        'TI', 'CH', 46.1944, 9.0175,  false, false);

-- --- Product categories, each with its subcategory list (native Postgres
-- array, read back as-is by db.py/psycopg2 - no CSV parsing needed) -------
INSERT INTO reference.product_categories (name, subcategories) VALUES
    ('Electronics',        ARRAY['Audio', 'Computing', 'Mobile Accessories', 'Cameras', 'Wearables']),
    ('Home & Kitchen',     ARRAY['Cookware', 'Small Appliances', 'Storage', 'Bedding', 'Lighting']),
    ('Sports & Outdoors',  ARRAY['Camping', 'Fitness', 'Cycling', 'Team Sports', 'Water Sports']),
    ('Toys & Games',       ARRAY['Board Games', 'Outdoor Toys', 'Puzzles', 'Building Sets', 'Action Figures']),
    ('Tools & Hardware',   ARRAY['Hand Tools', 'Power Tools', 'Fasteners', 'Measuring Tools', 'Workshop Storage']),
    ('Health & Beauty',    ARRAY['Skincare', 'Personal Care', 'Wellness', 'Haircare', 'Oral Care']),
    ('Office Supplies',    ARRAY['Stationery', 'Filing', 'Desk Accessories', 'Printing', 'Writing Instruments']),
    ('Automotive',         ARRAY['Interior', 'Exterior', 'Maintenance', 'Electronics', 'Tires & Wheels']);

-- --- Weather stations (unchanged from the pre-redesign 5 fixed locations,
-- kept independent of the CH-only delivery-zone scope reduction since
-- weather/delivery correlation isn't scoped to delivery zones) ------------
INSERT INTO reference.weather_stations (name, lat, lon) VALUES
    ('Biel',   47.14,   7.24),
    ('Bern',   46.95,   7.45),
    ('Paris',  48.8566, 2.3522),
    ('Berlin', 52.52,  13.405),
    ('Milan',  45.4642, 9.19);
