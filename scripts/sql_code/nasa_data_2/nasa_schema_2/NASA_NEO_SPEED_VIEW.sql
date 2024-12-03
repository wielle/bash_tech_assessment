create or replace view NASA_DATA_2.NASA_SCHEMA_2.NASA_NEO_SPEED_VIEW(
	ID,
	NEO_REFERENCE_ID,
	NAME,
	CLOSE_APPROACH_DATE_FULL,
	EPOCH_DATE_CLOSE_APPROACH,
	RELATIVE_VELOCITY_KILOMETERS_PER_SECOND,
	RELATIVE_VELOCITY_KILOMETERS_PER_HOUR,
	RELATIVE_VELOCITY_MILES_PER_HOUR,
	ORBITING_BODY
) as
SELECT 
    n.id,
    n.neo_reference_id,
    n.name,
    TO_TIMESTAMP_NTZ(c.value:close_approach_date_full::STRING, 'YYYY-MON-DD HH:MI') AS close_approach_date_full,
    c.value:epoch_date_close_approach::NUMBER AS epoch_date_close_approach,
    c.value:relative_velocity.kilometers_per_second::FLOAT AS relative_velocity_kilometers_per_second,
    c.value:relative_velocity.kilometers_per_hour::FLOAT AS relative_velocity_kilometers_per_hour,
    c.value:relative_velocity.miles_per_hour::FLOAT AS relative_velocity_miles_per_hour,
    c.value:orbiting_body::STRING AS orbiting_body
FROM 
    nasa_neo_data_2 n,
    LATERAL FLATTEN(input => PARSE_JSON(n.close_approach_data)) c;