-- =====================================================
-- DIM VACANCY ADDRESSES
-- =====================================================
INSERT INTO core.dim_vacancy_addresses(
    vacancy_id, raw_address, city, street, building, description, lat, lng
) VALUES %s
ON CONFLICT (vacancy_id) DO UPDATE 
SET
    raw_address = EXCLUDED.raw_address,
    city = EXCLUDED.city,
    street = EXCLUDED.street,
    building = EXCLUDED.building,
    description = EXCLUDED.description,
    lat = EXCLUDED.lat,
    lng = EXCLUDED.lng;

-- =====================================================
-- DIM VACANCY METRO STATIONS
-- =====================================================

INSERT INTO core.dim_vacancy_metro_stations
(vacancy_id, station_id, station_name, line_id, line_name, lat, lng)
VALUES %s
ON CONFLICT (vacancy_id, station_id) DO UPDATE SET
station_name = EXCLUDED.station_name,
line_id = EXCLUDED.line_id,
line_name = EXCLUDED.line_name,
lat = EXCLUDED.lat,
lng = EXCLUDED.lng;