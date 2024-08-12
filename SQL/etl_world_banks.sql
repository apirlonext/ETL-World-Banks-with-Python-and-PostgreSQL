--*************************************************************************************************	World Bank Data

																										--| Create Table
CREATE TABLE IF NOT EXISTS etl.world_bank_data (
	world_bank_id 			SERIAL PRIMARY KEY,
	bank_name 				TEXT,
	market_cap_usd 			FLOAT,
	last_modified_date 		DATE,
	batch_id 				TEXT,
	created_at 				TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at 				TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	active					BOOLEAN DEFAULT TRUE
);

																										--| Create Procedure & Body
CREATE OR REPLACE PROCEDURE etl.insert_or_update_world_bank_data (
	p_bank_name 			TEXT,
	p_market_cap_usd 		FLOAT,
	p_last_modified_date 	DATE,
	p_batch_id 				TEXT
)

LANGUAGE plpgsql
AS $$
DECLARE
	-- Initialize Counters
	v_no_update_count INT := 0;
	v_update_count INT := 0;
	v_new_inserts INT := 0;
BEGIN
	-- Check if there is an existing record for the bank
	IF EXISTS (SELECT 1 FROM etl.world_bank_data 
			   where bank_name = p_bank_name) 
	THEN
		-- Check if the record is currently active
		IF EXISTS (SELECT 1 FROM etl.world_bank_data 
				   WHERE bank_name = p_bank_name
					AND active = TRUE)
		THEN
			-- If the active record has the same carket_cap_usd, do nothing
			IF (SELECT market_cap_usd FROM etl.world_bank_data 
				WHERE bank_name = p_bank_name
				 AND active = TRUE) = p_market_cap_usd
			THEN
				v_no_update_count := v_no_update_count + 1;
			ELSE
				-- Update the active record if the market_cap_usd has changed
				UPDATE etl.world_bank_data
				SET market_cap_usd = p_market_cap_usd,
					last_modified_date = p_last_modified_date,
					batch_id = p_batch_id,
					updated_at = CURRENT_TIMESTAMP
				WHERE bank_name = p_bank_name
				 AND active = TRUE;
				v_update_count := v_update_count + 1;
			END IF;
		ELSE
			-- The record is currently inactive
			-- If the inactive record has the same market_cap_usd, reactivate it
			IF (SELECT market_cap_usd from etl.world_bank_data
				WHERE bank_name = p_bank_name
				 AND active = FALSE) = p_market_cap_usd
			THEN
				UPDATE etl.world_bank_data
				SET active = TRUE,
					batch_id = p_batch_id,
					updated_at = CURRENT_TIMESTAMP,
					last_modified_date = p_last_modified_date
				WHERE bank_name = p_bank_name
				 AND active = FALSE;
				RAISE NOTICE 'Reactivated record for %', p_bank_name;
			ELSE
				-- Insert a new active record if the market_cap_usd has changed
				INSERT INTO etl.world_bank_data (
					bank_name, 
					market_cap_usd, 
					last_modified_date, 
					batch_id, 
					created_at,
					updated_at,
					active)
				VALUES (
					p_bank_name, 
					p_market_cap_usd, 
					p_last_modified_date, 
					p_batch_id, 
					CURRENT_TIMESTAMP, 
					NULL,
					TRUE);
				RAISE NOTICE 'Insert new record, market_cap_usd has changed, for %', p_bank_name;
			END IF;
		END IF;
	ELSE
		-- Not existing record, insert a new active record
		INSERT INTO etl.world_bank_data (
			bank_name, 
			market_cap_usd, 
			last_modified_date, 
			batch_id, 
			created_at, 
			updated_at,
			active)
		VALUES (
			p_bank_name, 
			p_market_cap_usd, 
			p_last_modified_date, 
			p_batch_id, 
			CURRENT_TIMESTAMP, 
			NULL,
			TRUE);
		v_new_inserts := v_new_inserts + 1;
	END IF;
	
EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE 'World Bank Data: an error ocurred during the insert or update process: %', SQLERRM;
END;
$$;

																										--| Procedure Deactivate Old Bank Records
CREATE OR REPLACE PROCEDURE etl.deactivate_bank_records(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
	UPDATE etl.world_bank_data
	SET active = FALSE,
		updated_at = CURRENT_TIMESTAMP
	WHERE batch_id != p_batch_id
	 AND active = TRUE
	 AND updated_at IS NOT NULL
	 AND updated_at < CURRENT_DATE;
EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE 'World Bank Data: an error ocurred during the deactivation process: %', SQLERRM;
END;
$$;

																										--| Procedure Summary Load into World Bank Data
DROP PROCEDURE etl.insert_or_update_world_bank_data_summary;
CREATE OR REPLACE PROCEDURE etl.insert_or_update_world_bank_data_summary()
LANGUAGE plpgsql
AS $$
DECLARE
	v_no_update_count INT := 0;
	v_update_count INT := 0;
	v_new_inserts INT := 0;
	v_total_records INT := 0;
BEGIN
	-- Total Records Count
	SELECT COUNT(*) INTO v_total_records FROM etl.world_bank_data;

    SELECT COUNT(*) INTO v_new_inserts 
	FROM etl.world_bank_data 
	WHERE updated_at IS NULL 
		AND created_at = CURRENT_DATE;

    SELECT COUNT(*) INTO v_update_count 
	FROM etl.world_bank_data 
	WHERE updated_at IS NOT NULL
		or updated_at >= CURRENT_DATE;

    SELECT COUNT(*) INTO v_no_update_count 
	FROM etl.world_bank_data 
	WHERE (updated_at is NULL or updated_at < CURRENT_DATE) 
	AND created_at < CURRENT_DATE;

	-- Summary Notices
	RAISE NOTICE 'Number of new records inserted: %/%', v_new_inserts, v_total_records;
	RAISE NOTICE 'Number of records updated:  %/%', v_update_count, v_total_records;
	RAISE NOTICE 'Number of records with no updates needed:  %/%', v_no_update_count, v_total_records;

EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE 'World Bank Data: an error ocurred during the data summary process: %', SQLERRM;
END;
$$;

--*************************************************************************************************	Exchange Rates

																										--| Create Table
CREATE TABLE IF NOT EXISTS etl.exchanges_rates (
	exchange_rate_id 	SERIAL PRIMARY KEY,
	country				TEXT,
	currency			TEXT,
	exchange_rate		FLOAT,
	year				DATE,
	batch_id 			TEXT,
	created_at 			TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at 			TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

																										--| Create Procedure & Body
CREATE OR REPLACE PROCEDURE etl.insert_or_update_exchange_rates (
	p_country			TEXT,
	p_currency			TEXT,
	p_exchange_rate		FLOAT,
	p_year				DATE,
	p_batch_id			TEXT
)

LANGUAGE plpgsql
AS $$
DECLARE
	-- Initialize Counters
	v_no_update_count INT := 0;
	v_update_count INT := 0;
	v_new_inserts INT := 0;
BEGIN
	IF EXISTS (SELECT 1 FROM etl.exchanges_rates 
			   WHERE country = p_country 
			   	AND currency = p_currency
				AND year = p_year)
	THEN
		-- If record has the same exchange rate, do nothing
		IF (SELECT exchange_rate FROM etl.exchanges_rates 
			WHERE country = p_country 
			 AND currency = p_currency
			 AND year = p_year) = p_exchange_rate 
		THEN 
			v_no_update_count := v_no_update_count + 1;
		ELSE
			-- Update the record if the exchange rate has changed
			UPDATE etl.exchanges_rates
			SET exchange_rate = p_exchange_rate,
				batch_id = p_batch_id,
				update_at = CURRENT_TIMESTAMP
			WHERE country = p_country
			 AND currency = p_currency
			 AND year = p_year;
			v_update_count := v_update_count + 1;
		END IF;
	ELSE
		-- No existing record, insert new record
		INSERT INTO etl.exchanges_rates (
			country, 
			currency, 
			exchange_rate, 
			year, 
			batch_id, 
			created_at,
			updated_at)
		VALUES(
			p_country, 
			p_currency, 
			p_exchange_rate, 
			p_year, 
			p_batch_id, 
			CURRENT_TIMESTAMP,
			NULL);
		v_new_inserts := v_new_inserts + 1;
	END IF;
EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE 'Exchange rates: an error ocurred during the insert or update process: %', SQLERRM;
END;
$$;

																										--| Procedure Summary Load into Exchange Rates Data
DROP PROCEDURE etl.insert_or_update_exchange_rate_data_summary;
CREATE OR REPLACE PROCEDURE etl.insert_or_update_exchange_rate_data_summary()
LANGUAGE plpgsql
AS $$
DECLARE
	v_no_update_count INT := 0;
	v_update_count INT := 0;
	v_new_inserts INT := 0;
	v_total_records INT := 0;
BEGIN
	-- Total Records Count
	SELECT COUNT(*) INTO v_total_records FROM etl.exchanges_rates;

    SELECT COUNT(*) INTO v_new_inserts 
	FROM etl.exchanges_rates
	WHERE updated_at IS NULL 
		AND created_at = CURRENT_DATE;

    SELECT COUNT(*) INTO v_update_count 
	FROM etl.exchanges_rates
	WHERE updated_at IS NOT NULL
		or updated_at >= CURRENT_DATE;

    SELECT COUNT(*) INTO v_no_update_count 
	FROM etl.exchanges_rates
	WHERE (updated_at is NULL or updated_at < CURRENT_DATE) 
	AND created_at < CURRENT_DATE;

	-- Summary Notices
	RAISE NOTICE 'Number of new records inserted: %/%', v_new_inserts, v_total_records;
	RAISE NOTICE 'Number of records updated:  %/%', v_update_count, v_total_records;
	RAISE NOTICE 'Number of records with no updates needed:  %/%', v_no_update_count, v_total_records;

EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE 'Exchange Rates Data: an error ocurred during the data summary process: %', SQLERRM;
END;
$$;

--*************************************************************************************************	Log Table

																										--| Create Table
CREATE TABLE IF NOT EXISTS etl.process_logs (
	log_id 		SERIAL PRIMARY KEY,
	log_phase 	TEXT,
	message 	TEXT,
	datetime 	TIMESTAMP,
	batch_id 	TEXT,
	created_at 	TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
																										--| Create Procedure & Body
CREATE OR REPLACE PROCEDURE etl.insert_log (
	p_log_phase 	TEXT,
	p_message 		TEXT,
	p_log_datetime 	TIMESTAMP,
	p_batch_id 		TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO etl.process_logs (
		log_phase,
		message,
		datetime,
		batch_id,
		created_at)
	VALUES (
		p_log_phase,
		p_message,
		p_log_datetime,
		p_batch_id,
		CURRENT_TIMESTAMP);
EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE 'Log Process: an error ocurred during the deactivation process: %', SQLERRM;
END;
$$;