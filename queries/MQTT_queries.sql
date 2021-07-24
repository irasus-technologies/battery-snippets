-- PROCEDURE to insert in "transactions_packs"."simple_basic_measurement_cells"
-- DROP PROCEDURE "transactions_packs"."insert_sbmc"( bigint, numeric, numeric[], numeric[])

CREATE OR REPLACE PROCEDURE "transactions_packs"."insert_sbmc"(
	"_packNumber" bigint, 
	iat numeric, 
	"_cellVoltages" numeric[], 
	"_cellTemperatures" numeric[])
	LANGUAGE 'plpgsql'
	AS $BODY$
DECLARE
	cellcount_voltage INTEGER := array_length("_cellVoltages", 1);
	cellcount_temperature INTEGER := array_length("_cellTemperatures", 1);
	cell INTEGER := 1;
	_timestamp timestamp with time zone := to_timestamp(iat)::timestamptz(0);
BEGIN
	BEGIN
		WHILE cell <= cellcount_voltage LOOP
			EXECUTE FORMAT('INSERT INTO "transactions_packs"."simple_basic_measurements_cells" ("packNumber", "timestamp", "cellNumber", "measurementType", "measurementValue") VALUES (%s, %L, %s, 1, %s::NUMERIC / 1000)', "_packNumber", "_timestamp", cell, "_cellVoltages"[cell]);
			cell = cell + 1;
		END LOOP;
	END;
	cell := 1;
	BEGIN
		WHILE cell <= cellcount_temperature LOOP
			EXECUTE FORMAT('INSERT INTO "transactions_packs"."simple_basic_measurements_cells" ("packNumber", "timestamp", "cellNumber", "measurementType", "measurementValue") VALUES (%s, %L, %s, 5, %s)', "_packNumber", "_timestamp", cell, "_cellTemperatures"[cell]);
			cell = cell + 1;
		END LOOP;
	END;
END
$BODY$;


-- PROCEDURE to insert geographical values in "transactions_packs"."simple_meta_measurement_packs"
-- DROP PROCEDURE "transactions_packs"."insert_sbge"( bigint, numeric, json)

CREATE OR REPLACE PROCEDURE "transactions_packs"."insert_sbge"(
	"_packNumber" bigint,
	iat numeric,
	"_meta_measurements" json)
	LANGUAGE 'plpgsql'
	AS $BODY$
DECLARE
	--timestamp variables
	_timestamp timestamp with time zone;
	
	--Values for meta table
	measurementcount INT := 0;
	measurementindex INT := 1;
	measurementtype CHARACTER VARYING[];
	value_most_recent REAL := 0;
BEGIN
	_timestamp = transactions_packs."iatToTimestamp" (iat);
	
	--INSERT INTO "transactions_packs"."simple_meta_measurements_packs"	
  	EXECUTE FORMAT('SELECT COUNT(*) FROM "public"."types_measurements"') INTO measurementcount;
	EXECUTE FORMAT('SELECT ARRAY(SELECT types_measurements."measurementTypeAlias" FROM "public"."types_measurements" ORDER BY types_measurements."serialNumber")') INTO measurementtype;
	
	WHILE measurementindex <= measurementcount LOOP
		IF "_meta_measurements"->>measurementtype[measurementindex] IS NOT NULL THEN
			EXECUTE FORMAT('SELECT "measurementValue_numeric" FROM "transactions_packs"."simple_meta_measurements_packs" WHERE "packNumber"=%s AND "measurementType"=%s ORDER BY "timestamp" DESC LIMIT 1' , "_packNumber", measurementindex) INTO value_most_recent;
	
			IF ("_meta_measurements"->>measurementtype[measurementindex] <> value_most_recent::TEXT) OR (value_most_recent IS NULL) THEN
				EXECUTE FORMAT('INSERT INTO "transactions_packs"."simple_meta_measurements_packs" ("packNumber", "timestamp", "measurementType", "measurementValue_numeric") VALUES (%s, %L, %s, %s)', "_packNumber", _timestamp, measurementindex, "_meta_measurements"->>measurementtype[measurementindex]);
			END IF;
			value_most_recent = 0;
		END IF;
		measurementindex = measurementindex + 1;
	END LOOP;	
END
$_$;


-- PROCEDURE to insert electrical values in "transactions_packs"."simple_meta_measurement_packs"
-- DROP PROCEDURE "transactions_packs"."insert_sbee"( bigint, numeric, json)

CREATE OR REPLACE PROCEDURE "transactions_packs"."insert_sbee"(
	"_packNumber" bigint,
	iat numeric, 
	"_meta_measurements" json)
	LANGUAGE 'plpgsql'
	AS $BODY$
DECLARE
	--timestamp variables
	_timestamp timestamp with time zone;
	
	--Values for meta table
	measurementcount INT := 0;
	measurementindex INT := 1;
	measurementtype CHARACTER VARYING[];
	value_most_recent REAL := 0;	
BEGIN
  	_timestamp = transactions_packs."iatToTimestamp" (iat);
	
	--INSERT INTO "transactions_packs"."simple_meta_measurements_packs"	
  	EXECUTE FORMAT('SELECT COUNT(*) FROM "public"."types_measurements"') INTO measurementcount;
	EXECUTE FORMAT('SELECT ARRAY(SELECT types_measurements."measurementTypeAlias" FROM "public"."types_measurements" ORDER BY types_measurements."serialNumber")') INTO measurementtype;
	
	WHILE measurementindex <= measurementcount LOOP
		IF "_meta_measurements"->>measurementtype[measurementindex] IS NOT NULL THEN
			EXECUTE FORMAT('SELECT "measurementValue_numeric" FROM "transactions_packs"."simple_meta_measurements_packs" WHERE "packNumber"=%s AND "measurementType"=%s ORDER BY "timestamp" DESC LIMIT 1' , "_packNumber", measurementindex) INTO value_most_recent;
		
			IF ("_meta_measurements"->>measurementtype[measurementindex] <> value_most_recent::TEXT) OR (value_most_recent IS NULL) THEN
				EXECUTE FORMAT('INSERT INTO "transactions_packs"."simple_meta_measurements_packs" ("packNumber", "timestamp", "measurementType", "measurementValue_numeric") VALUES (%s, %L, %s, %s)', "_packNumber", _timestamp, measurementindex, "_meta_measurements"->>measurementtype[measurementindex]);
			END IF;
			value_most_recent = 0;
		END IF;
		measurementindex = measurementindex + 1;
	END LOOP;
END
$BODY$;
	