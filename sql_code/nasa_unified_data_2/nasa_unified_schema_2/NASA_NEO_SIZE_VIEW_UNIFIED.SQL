create or replace view NASA_UNIFIED_DATA_2.NASA_UNIFIED_SCHEMA_2.NASA_NEO_SIZE_VIEW_UNIFIED(
	ID,
	NEO_REFERENCE_ID,
	NAME,
	CLOSE_APPROACH_DATE_FULL,
	ABSOLUTE_MAGNITUDE_H,
	ESTIMATED_DIAMETER_KILOMETERS_ESTIMATED_DIAMETER_MIN,
	ESTIMATED_DIAMETER_KILOMETERS_ESTIMATED_DIAMETER_MAX,
	ESTIMATED_DIAMETER_METERS_ESTIMATED_DIAMETER_MIN,
	ESTIMATED_DIAMETER_METERS_ESTIMATED_DIAMETER_MAX,
	ESTIMATED_DIAMETER_MILES_ESTIMATED_DIAMETER_MIN,
	ESTIMATED_DIAMETER_MILES_ESTIMATED_DIAMETER_MAX,
	ESTIMATED_DIAMETER_FEET_ESTIMATED_DIAMETER_MIN,
	ESTIMATED_DIAMETER_FEET_ESTIMATED_DIAMETER_MAX,
	ORBITING_BODY
) as
SELECT 
    n.id,
    n.neo_reference_id,
    n.name,
    TO_TIMESTAMP_NTZ(c.value:close_approach_date_full::STRING, 'YYYY-MON-DD HH:MI') AS close_approach_date_full,
    n.absolute_magnitude_h,
    n.estimated_diameter_kilometers_estimated_diameter_min,
    n.estimated_diameter_kilometers_estimated_diameter_max,
    n.estimated_diameter_meters_estimated_diameter_min,
    n.estimated_diameter_meters_estimated_diameter_max,
    n.estimated_diameter_miles_estimated_diameter_min,
    n.estimated_diameter_miles_estimated_diameter_max,
    n.estimated_diameter_feet_estimated_diameter_min,
    n.estimated_diameter_feet_estimated_diameter_max,
    c.value:orbiting_body::STRING AS orbiting_body
FROM 
    nasa_neo_unified_data_2 n,
    LATERAL FLATTEN(input => PARSE_JSON(n.close_approach_data)) c;