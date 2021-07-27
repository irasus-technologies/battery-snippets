-- FUNTION FOR CALCULATING kWh and Ah while discharging for an individual pack
-- DROP FUNCTION transactions_packs.energy_charge_individual_pack_out (bigint, timestamp with time zone, timestamp with time zone )

CREATE OR REPLACE FUNCTION transactions_packs.energy_charge_individual_pack_out (pack bigint, from_time timestamp with time zone, to_time timestamp with time zone ) 
RETURNS numeric[] 
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE 
	vol_curr_arr real[] :=	(SELECT ARRAY_AGG(ARRAY["voltage", "current", capacity])
			 					FROM (SELECT "timestamp",
									  	AVG("voltage") OVER(ORDER BY timestamp
     									ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ) 
    									AS "voltage",
										AVG("current") OVER(ORDER BY timestamp
     									ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ) 
    									AS "current",
										AVG(capacity) OVER(ORDER BY timestamp
     									ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ) 
    									AS capacity,
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
								FROM (SELECT "timestamp",
									  AVG("voltage") OVER(ORDER BY timestamp
     									ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ) 
    									AS "voltage",
										AVG("current") OVER(ORDER BY timestamp
     									ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ) 
    									AS "current",
										AVG(capacity) OVER(ORDER BY timestamp
     									ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ) 
    									AS capacity,
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



---Moving average calculation of voltage, current and capacity for a given pack nad b/w given time interval

SELECT * ,
	AVG("voltage") OVER(ORDER BY timestamp
    	ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ) 
    	AS "voltage",
	AVG("current") OVER(ORDER BY timestamp
     	ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ) 
    	AS "current",
	AVG(capacity) OVER(ORDER BY timestamp
     	ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ) 
    	AS capacity,
	FROM transactions_packs.simple_basic_electrical_measurements_packs
	WHERE "packNumber" = pack AND
		"current" > 0 AND
		timestamp >= from_time AND
		timestamp <= to_time
		ORDER BY timestamp
		