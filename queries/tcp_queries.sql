-- Function for calculating distance traveled by individual pack
-- DROP FUNCTION  transactions_packs.distance_individual_pack (numeric, timestamp with time zone, timestamp with time zone )

CREATE OR REPLACE FUNCTION transactions_packs.distance_individual_pack ( pack numeric, from_time timestamp with time zone, to_time timestamp with time zone ) 
RETURNS numeric 
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE 
  	arr numeric[] :=	(SELECT ARRAY_AGG(ARRAY[latitude,longitude])
									FROM (SELECT "timestamp", "latitude", "longitude"
										FROM transactions_packs.simple_basic_geographical_measurements_packs
										WHERE "packNumber" = pack and
								   			timestamp >= from_time and
										   	timestamp <= to_time
							    		order by timestamp) sub);
	s numeric := 0;
BEGIN
	IF array_length(arr, 1) > 0 THEN
		FOR i IN array_lower(arr, 1) .. array_upper(arr, 1)-1
		LOOP
			s := s + (POINT(arr[i+1][1], arr[i+1][2])::POINT <@> POINT(arr[i][1], arr[i][2])::POINT);
		END LOOP;		
	END IF;
	s := round(s, 2);
	RETURN s; 
END;		
$BODY$;


-- FUNTION FOR CALCULATING kWh and Ah while discharging for an individual pack
-- DROP FUNCTION transactions_packs.energy_charge_individual_pack_out (bigint, timestamp with time zone, timestamp with time zone )

CREATE OR REPLACE FUNCTION transactions_packs.energy_charge_individual_pack_out (pack bigint, from_time timestamp with time zone, to_time timestamp with time zone ) 
RETURNS numeric[] 
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE 
	vol_curr_arr real[] :=	(SELECT ARRAY_AGG(ARRAY["voltage", "current", capacity])
			 					FROM (SELECT "timestamp", "voltage", "current", capacity
										FROM transactions_packs.simple_basic_electrical_measurements_packs
										WHERE "packNumber" = pack AND
							 					"current" < 0 AND
											   	timestamp >= from_time AND
											   	timestamp <= to_time
		     							ORDER BY timestamp) sub);

	arr_time timestamp with time zone[] := (SELECT ARRAY_AGG(timestamp)
				 							FROM (SELECT "timestamp", "voltage"
												FROM transactions_packs.simple_basic_electrical_measurements_packs
												WHERE "packNumber" = pack AND
								 			 		"current" < 0 AND
											  	 	timestamp >= from_time AND
											  	 	timestamp <= to_time
		     									ORDER BY timestamp) sub );
	kWh numeric := 0;
	Ah numeric := 0;
	kWh_capacity numeric := 0;
	ans numeric[];
	delta_time numeric := (SELECT EXTRACT ( SECOND FROM INTERVAL '30 seconds'));
BEGIN
	IF array_length(vol_curr_arr, 1) > 0 THEN
		FOR i IN array_lower(vol_curr_arr, 1) .. array_upper(vol_curr_arr, 1)-1
		LOOP				
			IF (to_seconds(arr_time[i+1]) - to_seconds(arr_time[i])) = delta_time THEN
				IF (vol_curr_arr[i][1]*vol_curr_arr[i][2]) <= (vol_curr_arr[i+1][1]*vol_curr_arr[i+1][2]) THEN
					kWh := kWh + (vol_curr_arr[i][1]*vol_curr_arr[i][2]*30) + (( vol_curr_arr[i+1][1]*vol_curr_arr[i+1][2] - vol_curr_arr[i][1]*vol_curr_arr[i][2])*15.0);
					Ah := Ah + (vol_curr_arr[i][2]*30) + (ABS(vol_curr_arr[i][2] - vol_curr_arr[i+1][2])*30/2);
				ELSE
					kWh := kWh + (vol_curr_arr[i+1][1]*vol_curr_arr[i+1][2]*30) + (ABS(vol_curr_arr[i][1]*vol_curr_arr[i][2] - vol_curr_arr[i+1][1]*vol_curr_arr[i+1][2])*30/2);
					Ah := Ah + (vol_curr_arr[i+1][2]*30) + (ABS(vol_curr_arr[i][2] - vol_curr_arr[i+1][2])*30/2);
				END IF;	
					kWh_capacity := kWh_capacity + (vol_curr_arr[i][1]*vol_curr_arr[i][3] - vol_curr_arr[i+1][1]*vol_curr_arr[i+1][3]);
				END IF;
		END LOOP;		
	END IF;
	ans[1] = round(ABS(kWh/3600), 2);
	ans[2] = round(ABS(Ah/3600), 2);
	ans[3] = round(kWh_capacity, 2);
	RETURN ans; 
	END;		
$BODY$;


-- FUNTION FOR CALCULATING kWh and Ah while charging for an individual pack
-- DROP FUNCTION transactions_packs.energy_charge_individual_pack_in ( bigint, timestamp with time zone, timestamp with time zone ) 

CREATE OR REPLACE FUNCTION transactions_packs.energy_charge_individual_pack_in (pack bigint, from_time timestamp with time zone, to_time timestamp with time zone ) 
RETURNS numeric[] 
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE 
	vol_curr_arr real[] :=  (SELECT ARRAY_AGG(ARRAY["voltage", "current", "capacity"])
								FROM (SELECT "timestamp", "voltage", "current", "capacity"
									FROM transactions_packs.simple_basic_electrical_measurements_packs
									WHERE "packNumber" = pack AND
								 		"current" > 0 AND
										timestamp >= from_time AND
										timestamp <= to_time
		     						ORDER BY timestamp) sub);

	arr_time timestamp with time zone[] := (SELECT ARRAY_AGG(timestamp)
												FROM (SELECT "timestamp", "voltage"
													FROM transactions_packs.simple_basic_electrical_measurements_packs
													WHERE "packNumber" = pack AND 
								 			   			"current" > 0 AND
											   			timestamp >= from_time AND
											   			timestamp <= to_time
		     							 			ORDER BY timestamp) sub );
	kWh numeric := 0;
	Ah numeric := 0;
	kWh_capacity numeric := 0;
	ans numeric[];
	delta_time double precision := (SELECT EXTRACT ( SECOND FROM INTERVAL '30 seconds'));		
BEGIN
	IF array_length(vol_curr_arr, 1) > 0 THEN
		FOR i IN array_lower(vol_curr_arr, 1) .. array_upper(vol_curr_arr, 1)-1
		LOOP
			IF (to_seconds(arr_time[i+1]) - to_seconds(arr_time[i])) = delta_time THEN
				IF (vol_curr_arr[i][1]*vol_curr_arr[i][2]) <= (vol_curr_arr[i+1][1]*vol_curr_arr[i+1][2]) THEN
					kWh := kWh + (vol_curr_arr[i][1]*vol_curr_arr[i][2]*30) + (( vol_curr_arr[i+1][1]*vol_curr_arr[i+1][2] - vol_curr_arr[i][1]*vol_curr_arr[i][2])*30/2);
					Ah := Ah + (vol_curr_arr[i][2]*30) + (ABS(vol_curr_arr[i][2] - vol_curr_arr[i+1][2])*30/2);
				ELSE
					kWh := kWh + (vol_curr_arr[i+1][1]*vol_curr_arr[i+1][2]*30) + (ABS(vol_curr_arr[i][1]*vol_curr_arr[i][2] - vol_curr_arr[i+1][1]*vol_curr_arr[i+1][2])*30/2);
					Ah := Ah + (vol_curr_arr[i+1][2]*30) + (ABS(vol_curr_arr[i][2] - vol_curr_arr[i+1][2])*30/2);
				END IF;	
				kWh_capacity := kWh_capacity + (vol_curr_arr[i][1]*vol_curr_arr[i][3] - vol_curr_arr[i+1][1]*vol_curr_arr[i+1][3]);
 			END IF;
		END LOOP;		
	END IF;
	ans[1] = round((kWh/3600), 2);
	ans[2] = round((Ah/3600), 2);
	ans[3] = round(kWh_capacity, 2);
RETURN ans; 
END;		
$BODY$


-- CREATING table daily 

CREATE TABLE IF NOT EXISTS transactions_packs.daily
(
    "packNumber" bigint NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    distance numeric NOT NULL,
    energy_in numeric NOT NULL,
    energy_out numeric NOT NULL,
	energy_in_capacity numeric NOT NULL,
	energy_out_capacity numeric NOT NULL,
    charge_in numeric NOT NULL,
    charge_out numeric NOT NULL,
    "createdAt" timestamp with time zone NOT NULL,
    "updatedAt" timestamp with time zone NOT NULL,
    "endTimestampEnergy" timestamp with time zone NOT NULL,
    "endTimestampDistance" timestamp with time zone NOT NULL,
    PRIMARY KEY ("packNumber", "timestamp")
)
WITH (
    OIDS = FALSE
);

-- DROP TABLE transactions_packs.daily


---
---Inserting kWh and Ah values in transactions_packs.daily
---
DO
$BODY$
DECLARE
	arr_packs bigint[] := (SELECT ARRAY_AGG(DISTINCT("packNumber"))
    							FROM transactions_packs.simple_basic_electrical_measurements_packs
								WHERE timestamp >= '2021-06-07T00:00:00+05:30'); -- taking data after '2021-06-07T00:00:00+05:30' 
									
	arr_days date[] := (SELECT ARRAY_AGG(DISTINCT(date(timestamp))) AS date_
								FROM transactions_packs.simple_basic_electrical_measurements_packs
								WHERE timestamp >= '2021-06-07T00:00:00+05:30' -- taking data after '2021-06-07T00:00:00+05:30'
								ORDER BY date_);
												
	distance numeric;
	energy_charge_in numeric[];
	energy_charge_out numeric[];
	updatedAt timestamp with time zone[];
	endTimestampEnergy timestamp with time zone;
	endTimestampDistance timestamp with time zone;
	start_time timestamp with time zone;
	end_time timestamp with time zone;
BEGIN 
	FOR i IN array_lower(arr_packs, 1) .. array_upper(arr_packs, 1)
	LOOP
		FOR j IN array_lower(arr_days, 1) .. array_upper(arr_days, 1)
		LOOP
			start_time := (arr_days[j] + '00:00:01'::time);
			end_time := (arr_days[j] + '23:59:59'::time);
			
			distance := distance_individual_pack(arr_packs[i], start_time, end_time);
			energy_charge_in := energy_charge_individual_pack_in(arr_packs[i], start_time, end_time);
			energy_charge_out := energy_charge_individual_pack_out(arr_packs[i], start_time, end_time);
			
			endTimestampEnergy := (SELECT MAX(timestamp) 
										FROM transactions_packs.simple_basic_electrical_measurements_packs
										WHERE timestamp < end_time);
								
			endTimestampDistance := (SELECT max(timestamp) 
							 			FROM transactions_packs.simple_basic_geographical_measurements_packs
										WHERE timestamp < end_time);
								
			INSERT INTO transactions_packs.daily VALUES (arr_packs[i], end_time, distance, energy_charge_in[1], energy_charge_out[1], energy_charge_in[3], energy_charge_out[3], energy_charge_in[2], energy_charge_out[2], now(), now(), endTimestampEnergy, endTimestampDistance);
		END LOOP;
	END LOOP;
END;
$BODY$


-- PROCEDURE: transactions_packs.insert__Geographical_Packs(numeric, integer, integer, double precision, double precision, numeric, numeric, smallint, numeric, text, numeric, json)
-- DROP PROCEDURE transactions_packs.insert__Geographical_Packs(numeric, integer, integer, double precision, double precision, numeric, numeric, smallint, numeric, text, numeric, json)

CREATE OR REPLACE PROCEDURE transactions_packs."insert__Geographical_Packs"(
	"IMEI" numeric,
	"time" integer,
	date integer,
	latitude double precision,
	longitude double precision,
	speed numeric,
	direction numeric,
	"satelliteCount" integer,
    odometer numeric,
	"packetType_G" text,
	"sequenceNumber_G" numeric,
	"_meta_measurements" json)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
	"_serialNumber" BIGINT;
	"_timestamp" TIMESTAMP WITH TIME ZONE;
	"_packNumber" BIGINT := 1;
	
	--Values for meta table
	measurementcount INT := 0;
	measurementindex INT := 1;
	measurementtype CHARACTER VARYING[];
	value_most_recent REAL := 0;
BEGIN
	EXECUTE FORMAT('SELECT TO_TIMESTAMP(LPAD(%L::TEXT, 6, ''0'') || '' '' || LPAD(%L::TEXT, 6, ''0''), ''DDMMYY HH24MISS'') + ''05:30 HOUR''::INTERVAL', "date", "time") INTO "_timestamp";
	EXECUTE FORMAT('SELECT * FROM "master_modems"."get__packNumber__IMEI"(%s)', "IMEI") INTO "_packNumber";
	
	--INSERTION INTO "transactions_packs"."simple_basic_geographical_measurements_packs" 
	EXECUTE FORMAT('INSERT INTO "transactions_packs"."simple_basic_geographical_measurements_packs" ("packNumber", "timestamp", "latitude", "longitude", "speed", "direction", "satelliteCount", "odometer", "packetType", "sequenceNumber", "createdAt", "updatedAt") VALUES (%s, %L, %s, %s, %s, %s, %s, %s, %L, %s, NOW(), NOW())', "_packNumber", "_timestamp", "latitude", "longitude", "speed", "direction", 0, 0, "packetType_G", "sequenceNumber_G");
	
	--INSERT INTO "transactions_packs"."simple_meta_measurements_packs"	
 	EXECUTE FORMAT('SELECT COUNT(*) FROM "public"."types_measurements"') INTO measurementcount;
	EXECUTE FORMAT('SELECT ARRAY(SELECT types_measurements."measurementTypeAlias" FROM "public"."types_measurements" ORDER BY types_measurements."serialNumber")') INTO measurementtype;
	
	WHILE measurementindex <= measurementcount LOOP
		IF "_meta_measurements"->>measurementtype[measurementindex] IS NOT NULL THEN
			EXECUTE FORMAT('SELECT "measurementValue_numeric" FROM "transactions_packs"."simple_meta_measurements_packs" WHERE "packNumber"=%s AND "measurementType"=%s ORDER BY "timestamp" DESC LIMIT 1' , "_packNumber", measurementindex) INTO value_most_recent;
			
			IF ("_meta_measurements"->>measurementtype[measurementindex] <> value_most_recent::TEXT) OR (value_most_recent IS NULL) THEN
				EXECUTE FORMAT('INSERT INTO "transactions_packs"."simple_meta_measurements_packs" ("packNumber", "timestamp", "measurementType", "measurementValue_numeric") VALUES (%s, now(), %s, %s)', "_packNumber", measurementindex, "_meta_measurements"->>measurementtype[measurementindex]);
			END IF;
			value_most_recent = 0;
		END IF;
		measurementindex = measurementindex + 1;
	END LOOP;
END;
$BODY$;


-- PROCEDURE: transactions_packs."insert__Electrical_Packs"( numeric, integer, integer, numeric, numeric, numeric, numeric, numeric, text, numeric, json, json)
-- DROP PROCEDURE transactions_packs."insert__Electrical_Packs"( numeric, integer, integer, numeric, numeric, numeric, numeric, numeric, text, numeric, json, json)

CREATE OR REPLACE PROCEDURE transactions_packs."insert__Electrical_Packs"(
	"IMEI" numeric,
	"time" integer,
	date integer,
	voltage numeric,
	current numeric,
	capacity numeric,
	"SoC" numeric,
	temperature numeric,
	"packetType_E" text,
	"sequenceNumber_E" numeric,
	"_basic_measurements_cells" json,
	"_meta_measurements" json)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
	"_serialNumber" BIGINT;
	"_timestamp" TIMESTAMP WITH TIME ZONE;
	"_packNumber" BIGINT := 1;
	
	--Values for meta table
	measurementcount INT := 0;
	measurementindex INT := 1;
	measurementtype CHARACTER VARYING[];
	value_most_recent REAL := 0;
BEGIN
	EXECUTE FORMAT('SELECT TO_TIMESTAMP(LPAD(%L::TEXT, 6, ''0'') || '' '' || LPAD(%L::TEXT, 6, ''0''), ''DDMMYY HH24MISS'') + ''05:30 HOUR''::INTERVAL', "date", "time") INTO "_timestamp";
	EXECUTE FORMAT('SELECT * FROM "master_modems"."get__packNumber__IMEI"(%s)', "IMEI") INTO "_packNumber";
	
	--INSERTING INTO "transactions_packs"."simple_basic_electrical_measurements_packs"
	EXECUTE FORMAT('INSERT INTO "transactions_packs"."simple_basic_electrical_measurements_packs" ("packNumber", "timestamp", "voltage", "current", "charge", "SoC", "temperature", "packetType", "sequenceNumber", "createdAt", "updatedAt") VALUES (%s, %L, %s, %s, %s, %s, %s, %L, %s, NOW(), NOW())', "_packNumber", "_timestamp", "voltage", "current", "capacity", "SoC", "temperature", "packetType_E", "sequenceNumber_E");

	--INSERT INTO "transactions_packs"."simple_basic_measurements_cells"
	EXECUTE FORMAT('INSERT INTO "transactions_packs"."simple_basic_measurements_cells" ("packNumber", "timestamp", "cellNumber", "measurementType", "measurementValue") VALUES (%1$s, %2$L, %3$s, 1, %4$s), (%1$s, %2$L, %5$s, 5, %6$s)', "_packNumber", "_timestamp", "_basic_measurements_cells"->>'min_voltage_cell_number', "_basic_measurements_cells"->>'min_voltage_cell_value', "_basic_measurements_cells"->>'min_temperature_cell_number', "_basic_measurements_cells"->>'min_temperature_cell_value');
	
	IF "_basic_measurements_cells"->>'min_temperature_cell_number' != "_basic_measurements_cells"->>'max_temperature_cell_number' THEN
		EXECUTE FORMAT('INSERT INTO "transactions_packs"."simple_basic_measurements_cells" ("packNumber", "timestamp", "cellNumber", "measurementType", "measurementValue") VALUES (%1$s, %2$L, %3$s, 5, %4$s)', "_packNumber", "_timestamp", "_basic_measurements_cells"->>'max_temperature_cell_number', "_basic_measurements_cells"->>'max_temperature_cell_value');
	END IF;
	IF "_basic_measurements_cells"->>'min_voltage_cell_number' != "_basic_measurements_cells"->>'max_voltage_cell_number' THEN
		EXECUTE FORMAT('INSERT INTO "transactions_packs"."simple_basic_measurements_cells" ("packNumber", "timestamp", "cellNumber", "measurementType", "measurementValue") VALUES (%1$s, %2$L, %3$s, 1, %4$s)', "_packNumber", "_timestamp", "_basic_measurements_cells"->>'max_voltage_cell_number', "_basic_measurements_cells"->>'max_voltage_cell_value');
	END IF;
	
	--INSERT INTO "transactions_packs"."simple_meta_measurements_packs"
  	EXECUTE FORMAT('SELECT COUNT(*) FROM "public"."types_measurements"') INTO measurementcount;
	EXECUTE FORMAT('SELECT ARRAY(SELECT types_measurements."measurementTypeAlias" FROM "public"."types_measurements" ORDER BY types_measurements."serialNumber")') INTO measurementtype;
	
	WHILE measurementindex <= measurementcount LOOP
		IF "_meta_measurements"->>measurementtype[measurementindex] IS NOT NULL THEN
			EXECUTE FORMAT('SELECT "measurementValue_numeric" FROM "transactions_packs"."simple_meta_measurements_packs" WHERE "packNumber"=%s AND "measurementType"=%s ORDER BY "timestamp" DESC LIMIT 1' , "_packNumber", measurementindex) INTO value_most_recent;
			
			IF ("_meta_measurements"->>measurementtype[measurementindex] <> value_most_recent::TEXT) OR (value_most_recent IS NULL) THEN
				EXECUTE FORMAT('INSERT INTO "transactions_packs"."simple_meta_measurements_packs" ("packNumber", "timestamp", "measurementType", "measurementValue_numeric") VALUES (%s, now(), %s, %s)', "_packNumber", measurementindex, "_meta_measurements"->>measurementtype[measurementindex]);
			END IF;
			value_most_recent = 0;
		END IF;
		measurementindex = measurementindex + 1;
	END LOOP;
END;
$BODY$;


