
-- CREATE DATABASE delivery_data
--     WITH
--     OWNER = postgres
--     ENCODING = 'UTF8'
--     LC_COLLATE = 'English_India.1252'
--     LC_CTYPE = 'English_India.1252'
--     LOCALE_PROVIDER = 'libc'
--     TABLESPACE = pg_default
--     CONNECTION LIMIT = -1
--     IS_TEMPLATE = False;


--------------------------------


-- CREATE TABLE my_static_data (
-- 		static_id SERIAL PRIMARY KEY,
-- 		isin VARCHAR(20) NOT NULL UNIQUE,
-- 		fininstrmid VARCHAR(10),
-- 		fininstrmid_b VARCHAR(10),
-- 		tckrsymb VARCHAR(20),
-- 		tckrsymb_b VARCHAR(20),
-- 		src VARCHAR(10),
-- 		src_b VARCHAR(10),
-- 		fininstrmnm VARCHAR(50) );




--------------------------------


-- CREATE TABLE my_daily_data (
-- 		ISIN VARCHAR(20) NOT NULL,
-- 		TradDt DATE NOT NULL,
-- 		sctysrs VARCHAR(10),
-- 		sctysrs_b VARCHAR(10),
-- 		opnpric DECIMAL(12,2),
-- 		hghpric DECIMAL(12,2),
-- 		lwpric DECIMAL(12,2),
-- 		clspric DECIMAL(12,2),
-- 		lastpric DECIMAL(12,2),
-- 		prvsclsgpric DECIMAL(12,2),
-- 		sttlmpric DECIMAL(12,2),
-- 		ttltradgvol BIGINT,
-- 		ttltrfval BIGINT,
-- 		ttlnboftxsexctd BIGINT,
-- 		qty_del BIGINT,
-- 		delvry_trnovr BIGINT,
-- 		ttltradgvol_b BIGINT,
-- 		ttltrfval_b BIGINT,
-- 		ttlnboftxsexctd_b BIGINT,
-- 		qty_del_b BIGINT,
-- 		delvry_trnovr_b BIGINT,
-- 		sum_ttltradgvol BIGINT,
-- 		sum_ttltrfval BIGINT,
-- 		sum_ttlnboftxsexctd BIGINT,
-- 		sum_del_qty BIGINT,
-- 		sum_delvry_trnovr BIGINT,
-- 		del_per DECIMAL(12,2),
-- 		avg_price DECIMAL(12,2),
-- 		avg_qty_per_order BIGINT,
-- 		avg_order_price DECIMAL(12,2),
-- 		close_to_avg DECIMAL(12,2),
-- 		close_avg_perc DECIMAL(12,2),
-- 		othr_trds VARCHAR(20),
-- 		othr_trds_vol BIGINT,
-- 		othr_trds_val BIGINT,
-- 		othr_trds_txsexctd BIGINT,
-- 		FOREIGN KEY (ISIN) REFERENCES my_static_data(ISIN) ON DELETE CASCADE,
-- 		CONSTRAINT my_daily_data_pkey PRIMARY KEY (ISIN, TradDt, sctysrs, sctysrs_b)
-- 		)
-- 		PARTITION BY RANGE (TradDt);

--------------------------------
-- CREATE TABLE my_daily_data_q1_2024 PARTITION OF my_daily_data
-- 	FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');

-- CREATE TABLE my_daily_data_q2_2024 PARTITION OF my_daily_data
-- 	FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
	
-- CREATE TABLE my_daily_data_q3_2024 PARTITION OF my_daily_data
-- 	FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');
	
-- CREATE TABLE my_daily_data_q4_2024 PARTITION OF my_daily_data
-- 	FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');


	
-- CREATE TABLE my_daily_data_q1_2025 PARTITION OF my_daily_data
-- 	FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');


-- CREATE TABLE my_daily_data_q2_2025 PARTITION OF my_daily_data
-- 	FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');


	
-- CREATE TABLE my_daily_data_q3_2025 PARTITION OF my_daily_data
-- 	FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');


	
-- CREATE TABLE my_daily_data_q4_2025 PARTITION OF my_daily_data
-- 	FOR VALUES FROM ('2025-10-01') TO ('2026-01-01');

--------------------------------



-- CREATE TABLE financial_year (
--     id SERIAL PRIMARY KEY,
--     static_id INTEGER NOT NULL,
--     fin_year INT NOT NULL,
--     revenue DECIMAL(18, 2),
--     other_income DECIMAL(18, 2),
--     total_income DECIMAL(18, 2),
--     expenditure DECIMAL(18, 2),
--     interest DECIMAL(18, 2),
--     pbdt DECIMAL(18, 2),
--     depreciation DECIMAL(18, 2),
--     pbt DECIMAL(18, 2),
--     tax DECIMAL(18, 2),
--     net_profit DECIMAL(18, 2),
--     equity DECIMAL(18, 2),
--     eps DECIMAL(18, 2),
--     ceps DECIMAL(18, 2),
--     opm_perc DECIMAL(5, 2),
--     npm_perc DECIMAL(5, 2),
--     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     FOREIGN KEY (static_id) REFERENCES my_static_data(static_id),
--     UNIQUE (static_id, fin_year)
-- );


-- --------------------------------
