create or replace view NASA_DATA_2.NASA_SCHEMA_2.NASA_NEO_CLOSE_APPROACH_VIEW(
	ID,
	NEO_REFERENCE_ID,
	NAME,
	NASA_JPL_URL,
	ABSOLUTE_MAGNITUDE_H,
	IS_POTENTIALLY_HAZARDOUS_ASTEROID,
	IS_SENTRY_OBJECT,
	LINKS_SELF,
	ESTIMATED_DIAMETER_KILOMETERS_ESTIMATED_DIAMETER_MIN,
	ESTIMATED_DIAMETER_KILOMETERS_ESTIMATED_DIAMETER_MAX,
	ESTIMATED_DIAMETER_METERS_ESTIMATED_DIAMETER_MIN,
	ESTIMATED_DIAMETER_METERS_ESTIMATED_DIAMETER_MAX,
	ESTIMATED_DIAMETER_MILES_ESTIMATED_DIAMETER_MIN,
	ESTIMATED_DIAMETER_MILES_ESTIMATED_DIAMETER_MAX,
	ESTIMATED_DIAMETER_FEET_ESTIMATED_DIAMETER_MIN,
	ESTIMATED_DIAMETER_FEET_ESTIMATED_DIAMETER_MAX,
	SENTRY_DATA,
	CLOSE_APPROACH_DATE,
	CLOSE_APPROACH_DATE_FULL,
	EPOCH_DATE_CLOSE_APPROACH,
	RELATIVE_VELOCITY_KILOMETERS_PER_SECOND,
	RELATIVE_VELOCITY_KILOMETERS_PER_HOUR,
	RELATIVE_VELOCITY_MILES_PER_HOUR,
	MISS_DISTANCE_ASTRONOMICAL,
	MISS_DISTANCE_LUNAR,
	MISS_DISTANCE_KILOMETERS,
	MISS_DISTANCE_MILES,
	ORBITING_BODY
) as
SELECT 
    n.id,
    n.neo_reference_id,
    n.name,
    n.nasa_jpl_url,
    n.absolute_magnitude_h,
    n.is_potentially_hazardous_asteroid,
    n.is_sentry_object,
    n.links_self,
    n.estimated_diameter_kilometers_estimated_diameter_min,
    n.estimated_diameter_kilometers_estimated_diameter_max,
    n.estimated_diameter_meters_estimated_diameter_min,
    n.estimated_diameter_meters_estimated_diameter_max,
    n.estimated_diameter_miles_estimated_diameter_min,
    n.estimated_diameter_miles_estimated_diameter_max,
    n.estimated_diameter_feet_estimated_diameter_min,
    n.estimated_diameter_feet_estimated_diameter_max,
    n.sentry_data,
    TO_DATE(c.value:close_approach_date::STRING, 'YYYY-MM-DD') AS close_approach_date,
    TO_TIMESTAMP_NTZ(c.value:close_approach_date_full::STRING, 'YYYY-MON-DD HH:MI') AS close_approach_date_full,
    c.value:epoch_date_close_approach::NUMBER AS epoch_date_close_approach,
    c.value:relative_velocity.kilometers_per_second::FLOAT AS relative_velocity_kilometers_per_second,
    c.value:relative_velocity.kilometers_per_hour::FLOAT AS relative_velocity_kilometers_per_hour,
    c.value:relative_velocity.miles_per_hour::FLOAT AS relative_velocity_miles_per_hour,
    c.value:miss_distance.astronomical::FLOAT AS miss_distance_astronomical,
    c.value:miss_distance.lunar::FLOAT AS miss_distance_lunar,
    c.value:miss_distance.kilometers::FLOAT AS miss_distance_kilometers,
    c.value:miss_distance.miles::FLOAT AS miss_distance_miles,
    c.value:orbiting_body::STRING AS orbiting_body
FROM 
    nasa_neo_data_2 n,
    LATERAL FLATTEN(input => PARSE_JSON(n.close_approach_data)) c;