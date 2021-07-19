-- FUNTION FOR CALCULATING sessions for an individual pack
-- DROP FUNCTION transactions_packs.session_individual_pack ( bigint, timestamp with time zone, timestamp with time zone)

CREATE OR REPLACE FUNCTION transactions_packs.session_individual_pack(pack bigint, from_time timestamp with time zone, to_time timestamp with time zone ) 
	RETURNS void as 
$$
	DECLARE
	-- get timestamp with non zero current
	time_arr timestamp with time zone[] :=  (SELECT ARRAY_AGG("timestamp")
				 				FROM (SELECT "timestamp", "current"
										FROM transactions_packs.simple_basic_electrical_measurements_packs
										WHERE "packNumber" = pack and 
									  		  "current" != 0 and
											   timestamp >= from_time and
											   timestamp <= to_time
		     							 ORDER BY timestamp) sub);
	-- get all non zero currents
	curr_arr numeric[] :=  (SELECT ARRAY_AGG("current")
				 				FROM (SELECT "timestamp", "current"
										FROM transactions_packs.simple_basic_electrical_measurements_packs
										WHERE "packNumber" = pack and 
									  		  "current" != 0 and
											   timestamp >= from_time and
											   timestamp <= to_time
		     							 ORDER BY timestamp) sub);
	status numeric[];
	time_points timestamp with time zone[];
	flag_status numeric := 1;
	flag_time numeric := 1;
	delta_time double precision := (SELECT EXTRACT ( SECOND FROM INTERVAL '30 seconds'));
	
	BEGIN
	
		IF curr_arr[1] < 0 THEN
			status[1] = -1;	-- discharging
		else
			status[1] = 1;	-- charging
		END IF;
		time_points[1] = time_arr[1];  --- end tiem for first session
		
		IF array_length(time_arr, 1) > 0 THEN
			FOR i IN array_lower(time_arr, 1) .. array_upper(time_arr, 1)
			LOOP				
				IF (to_seconds(time_arr[i+1]) - to_seconds(time_arr[i])) > delta_time*6 OR status[flag_status]*curr_arr[i] < 0 THEN	
					time_points[flag_time + 1 ] := time_arr[i]; -- end time for current session
					flag_status := flag_status + 1;
					IF curr_arr[i+1] < 0 THEN
						status[flag_status] = -1;	-- discharging
					else
						status[flag_status] = 1;	-- charging
					END IF;
					time_points[flag_time +2] := time_arr[i+1]; -- start time for next session
					flag_time := flag_time + 2;					
				END IF;	
			END LOOP;
			time_points[flag_time +1] := time_arr[array_upper(time_arr, 1)];  -- end time for last session
		END IF;
		
		-- Inserting into the sessin status and time marks in table 
		FOR i IN array_lower(status, 1) .. array_upper(status, 1)
		LOOP
			--raise notice '% and % and % and %', pack, status[i], time_points[2*i-1], time_points[2*i];
			INSERT INTO transactions_packs.session_data VALUES (pack, status[i], time_points[2*i-1], time_points[2*i] );
		END LOOP;
		
	END;		
$$
LANGUAGE 'plpgsql';


-- test call
SELECT transactions_packs."session_individual_pack"(352277811387214, '2021-05-30 00:00:36+05:30', '2021-05-30 23:59:36+05:30')


-- TABLE
CREATE TABLE transactions_packs.session_data(
		"packNumber" bigint,
		session_status numeric,
		start_time timestamp with time zone,
		end_time timestamp with time zone
)
WITH (
OIDS = FALSE 
);

SELECT * FROM transactions_packs.session_data;

DELETE FROM transactions_packs.session_data;
