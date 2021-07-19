--PROCEDURE to insert in "transactions_packs"."simple_basic_measurement_cells"
CREATE PROCEDURE "transactions_packs"."insert_sbmc"("_packNumber" bigint, iat numeric, "_cellVoltages" numeric[], "_cellTemperatures" numeric[])
	LANGUAGE "plpgsql"
	AS $_$
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
$_$;


--PROCEDURE to insert geographical values in "transactions_packs"."simple_meta_measurement_packs"
CREATE OR REPLACE PROCEDURE "transactions_packs"."insert_sbge"("_packNumber" bigint, iat numeric, "_meta_measurements" json)
	LANGUAGE "plpgsql"
	AS $_$
DECLARE
  
  --timestamp variables
  _temp numeric := iat;
	_count numeric :=0;
	_timestamp timestamp with time zone;
	
	--Values for meta table
	measurementcount INT := 0;
	measurementindex INT := 1;
	measurementtype CHARACTER VARYING[];
	value_most_recent REAL := 0;
	
BEGIN

  while _temp != 0 loop
		_temp := round(_temp/10, 0)::numeric;
		_count := _count + 1;
	end loop;
	
	if _count = 10::numeric then 
		_timestamp := to_timestamp(iat)::timestamptz(0);
	else
		iat := round(iat/1000, 0)::numeric;
		_timestamp := to_timestamp(iat)::timestamptz(0);
	end if;
	
	--INSERT INTO "transactions_packs"."simple_meta_measurements_packs"	
  EXECUTE FORMAT('SELECT COUNT(*) FROM "public"."types_measurements"') INTO measurementcount;
	EXECUTE FORMAT('SELECT ARRAY(SELECT types_measurements."measurementTypeAlias" FROM "public"."types_measurements" ORDER BY types_measurements."serialNumber")') INTO measurementtype;
	
	WHILE measurementindex <= measurementcount LOOP
		IF "_meta_measurements"->>measurementtype[measurementindex] IS NOT NULL THEN
			--
			EXECUTE FORMAT('SELECT "measurementValue_numeric" FROM "transactions_packs"."simple_meta_measurements_packs" WHERE "packNumber"=%s AND "measurementType"=%s ORDER BY "timestamp" DESC LIMIT 1' , "_packNumber", measurementindex) INTO value_most_recent;
			--
			IF ("_meta_measurements"->>measurementtype[measurementindex] <> value_most_recent::TEXT) OR (value_most_recent IS NULL) THEN
				EXECUTE FORMAT('INSERT INTO "transactions_packs"."simple_meta_measurements_packs" ("packNumber", "timestamp", "measurementType", "measurementValue_numeric") VALUES (%s, %L, %s, %s)', "_packNumber", _timestamp, measurementindex, "_meta_measurements"->>measurementtype[measurementindex]);
			END IF;
			value_most_recent = 0;
		END IF;
		measurementindex = measurementindex + 1;
	END LOOP;
	
	
END
$_$;
	


--PROCEDURE to insert electrical values in "transactions_packs"."simple_meta_measurement_packs"
CREATE OR REPLACE PROCEDURE "transactions_packs"."insert_sbee"("_packNumber" bigint, iat numeric, "_meta_measurements" json)
	LANGUAGE "plpgsql"
	AS $_$
DECLARE
  
  --timestamp variables
  _temp numeric := iat;
	_count numeric :=0;
	_timestamp timestamp with time zone;
	
	--Values for meta table
	measurementcount INT := 0;
	measurementindex INT := 1;
	measurementtype CHARACTER VARYING[];
	value_most_recent REAL := 0;
	
BEGIN

  while _temp != 0 loop
		_temp := round(_temp/10, 0)::numeric;
		_count := _count + 1;
	end loop;
	
	if _count = 10::numeric then 
		_timestamp := to_timestamp(iat)::timestamptz(0);
	else
		iat := round(iat/1000, 0)::numeric;
		_timestamp := to_timestamp(iat)::timestamptz(0);
	end if;
	
	--INSERT INTO "transactions_packs"."simple_meta_measurements_packs"	
  EXECUTE FORMAT('SELECT COUNT(*) FROM "public"."types_measurements"') INTO measurementcount;
	EXECUTE FORMAT('SELECT ARRAY(SELECT types_measurements."measurementTypeAlias" FROM "public"."types_measurements" ORDER BY types_measurements."serialNumber")') INTO measurementtype;
	
	WHILE measurementindex <= measurementcount LOOP
		IF "_meta_measurements"->>measurementtype[measurementindex] IS NOT NULL THEN
			--
			EXECUTE FORMAT('SELECT "measurementValue_numeric" FROM "transactions_packs"."simple_meta_measurements_packs" WHERE "packNumber"=%s AND "measurementType"=%s ORDER BY "timestamp" DESC LIMIT 1' , "_packNumber", measurementindex) INTO value_most_recent;
			--
			IF ("_meta_measurements"->>measurementtype[measurementindex] <> value_most_recent::TEXT) OR (value_most_recent IS NULL) THEN
				EXECUTE FORMAT('INSERT INTO "transactions_packs"."simple_meta_measurements_packs" ("packNumber", "timestamp", "measurementType", "measurementValue_numeric") VALUES (%s, %L, %s, %s)', "_packNumber", _timestamp, measurementindex, "_meta_measurements"->>measurementtype[measurementindex]);
			END IF;
			value_most_recent = 0;
		END IF;
		measurementindex = measurementindex + 1;
	END LOOP;
	
	
END
$_$;
	