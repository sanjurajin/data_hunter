


SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


CREATE SCHEMA public;


ALTER SCHEMA public OWNER TO pg_database_owner;


COMMENT ON SCHEMA public IS 'standard public schema';


SET default_tablespace = '';

SET default_table_access_method = heap;


CREATE TABLE public.financial_year (
    id integer NOT NULL,
    static_id integer NOT NULL,
    fin_year integer NOT NULL,
    revenue numeric(18,2),
    other_income numeric(18,2),
    total_income numeric(18,2),
    expenditure numeric(18,2),
    interest numeric(18,2),
    pbdt numeric(18,2),
    depreciation numeric(18,2),
    pbt numeric(18,2),
    tax numeric(18,2),
    net_profit numeric(18,2),
    equity numeric(18,2),
    eps numeric(18,2),
    ceps numeric(18,2),
    opm_perc numeric(5,2),
    npm_perc numeric(5,2),
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);





CREATE SEQUENCE public.financial_year_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.financial_year_id_seq OWNER TO postgres;


ALTER SEQUENCE public.financial_year_id_seq OWNED BY public.financial_year.id;



CREATE TABLE public.financials_qtr (
    id integer NOT NULL,
    static_id integer NOT NULL,
    qtr_start_date date NOT NULL,
    revenue numeric(18,2),
    other_income numeric(18,2),
    total_income numeric(18,2),
    expenditure numeric(18,2),
    interest numeric(18,2),
    pbdt numeric(18,2),
    depreciation numeric(18,2),
    pbt numeric(18,2),
    tax numeric(18,2),
    net_profit numeric(18,2),
    equity numeric(18,2),
    eps numeric(18,2),
    ceps numeric(18,2),
    opm_perc numeric(5,2),
    npm_perc numeric(5,2),
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);





CREATE SEQUENCE public.financials_qtr_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.financials_qtr_id_seq OWNER TO postgres;


ALTER SEQUENCE public.financials_qtr_id_seq OWNED BY public.financials_qtr.id;



CREATE TABLE public.my_daily_data (
    isin character varying(20) NOT NULL,
    traddt date NOT NULL,
    sctysrs character varying(10) NOT NULL,
    sctysrs_b character varying(10) NOT NULL,
    opnpric numeric(12,2),
    hghpric numeric(12,2),
    lwpric numeric(12,2),
    clspric numeric(12,2),
    lastpric numeric(12,2),
    prvsclsgpric numeric(12,2),
    sttlmpric numeric(12,2),
    ttltradgvol bigint,
    ttltrfval bigint,
    ttlnboftxsexctd bigint,
    qty_del bigint,
    delvry_trnovr bigint,
    ttltradgvol_b bigint,
    ttltrfval_b bigint,
    ttlnboftxsexctd_b bigint,
    qty_del_b bigint,
    delvry_trnovr_b bigint,
    sum_ttltradgvol bigint,
    sum_ttltrfval bigint,
    sum_ttlnboftxsexctd bigint,
    sum_del_qty bigint,
    sum_delvry_trnovr bigint,
    del_per numeric(12,2),
    avg_price numeric(12,2),
    avg_qty_per_order bigint,
    avg_order_price numeric(12,2),
    close_to_avg numeric(12,2),
    close_avg_perc numeric(12,2),
    othr_trds character varying(20),
    othr_trds_vol bigint,
    othr_trds_val bigint,
    othr_trds_txsexctd bigint
)
PARTITION BY RANGE (traddt);





CREATE TABLE public.my_daily_data_q1_2024 (
    isin character varying(20) NOT NULL,
    traddt date NOT NULL,
    sctysrs character varying(10) NOT NULL,
    sctysrs_b character varying(10) NOT NULL,
    opnpric numeric(12,2),
    hghpric numeric(12,2),
    lwpric numeric(12,2),
    clspric numeric(12,2),
    lastpric numeric(12,2),
    prvsclsgpric numeric(12,2),
    sttlmpric numeric(12,2),
    ttltradgvol bigint,
    ttltrfval bigint,
    ttlnboftxsexctd bigint,
    qty_del bigint,
    delvry_trnovr bigint,
    ttltradgvol_b bigint,
    ttltrfval_b bigint,
    ttlnboftxsexctd_b bigint,
    qty_del_b bigint,
    delvry_trnovr_b bigint,
    sum_ttltradgvol bigint,
    sum_ttltrfval bigint,
    sum_ttlnboftxsexctd bigint,
    sum_del_qty bigint,
    sum_delvry_trnovr bigint,
    del_per numeric(12,2),
    avg_price numeric(12,2),
    avg_qty_per_order bigint,
    avg_order_price numeric(12,2),
    close_to_avg numeric(12,2),
    close_avg_perc numeric(12,2),
    othr_trds character varying(20),
    othr_trds_vol bigint,
    othr_trds_val bigint,
    othr_trds_txsexctd bigint
);





CREATE TABLE public.my_daily_data_q1_2025 (
    isin character varying(20) NOT NULL,
    traddt date NOT NULL,
    sctysrs character varying(10) NOT NULL,
    sctysrs_b character varying(10) NOT NULL,
    opnpric numeric(12,2),
    hghpric numeric(12,2),
    lwpric numeric(12,2),
    clspric numeric(12,2),
    lastpric numeric(12,2),
    prvsclsgpric numeric(12,2),
    sttlmpric numeric(12,2),
    ttltradgvol bigint,
    ttltrfval bigint,
    ttlnboftxsexctd bigint,
    qty_del bigint,
    delvry_trnovr bigint,
    ttltradgvol_b bigint,
    ttltrfval_b bigint,
    ttlnboftxsexctd_b bigint,
    qty_del_b bigint,
    delvry_trnovr_b bigint,
    sum_ttltradgvol bigint,
    sum_ttltrfval bigint,
    sum_ttlnboftxsexctd bigint,
    sum_del_qty bigint,
    sum_delvry_trnovr bigint,
    del_per numeric(12,2),
    avg_price numeric(12,2),
    avg_qty_per_order bigint,
    avg_order_price numeric(12,2),
    close_to_avg numeric(12,2),
    close_avg_perc numeric(12,2),
    othr_trds character varying(20),
    othr_trds_vol bigint,
    othr_trds_val bigint,
    othr_trds_txsexctd bigint
);





CREATE TABLE public.my_daily_data_q2_2024 (
    isin character varying(20) NOT NULL,
    traddt date NOT NULL,
    sctysrs character varying(10) NOT NULL,
    sctysrs_b character varying(10) NOT NULL,
    opnpric numeric(12,2),
    hghpric numeric(12,2),
    lwpric numeric(12,2),
    clspric numeric(12,2),
    lastpric numeric(12,2),
    prvsclsgpric numeric(12,2),
    sttlmpric numeric(12,2),
    ttltradgvol bigint,
    ttltrfval bigint,
    ttlnboftxsexctd bigint,
    qty_del bigint,
    delvry_trnovr bigint,
    ttltradgvol_b bigint,
    ttltrfval_b bigint,
    ttlnboftxsexctd_b bigint,
    qty_del_b bigint,
    delvry_trnovr_b bigint,
    sum_ttltradgvol bigint,
    sum_ttltrfval bigint,
    sum_ttlnboftxsexctd bigint,
    sum_del_qty bigint,
    sum_delvry_trnovr bigint,
    del_per numeric(12,2),
    avg_price numeric(12,2),
    avg_qty_per_order bigint,
    avg_order_price numeric(12,2),
    close_to_avg numeric(12,2),
    close_avg_perc numeric(12,2),
    othr_trds character varying(20),
    othr_trds_vol bigint,
    othr_trds_val bigint,
    othr_trds_txsexctd bigint
);





CREATE TABLE public.my_daily_data_q2_2025 (
    isin character varying(20) NOT NULL,
    traddt date NOT NULL,
    sctysrs character varying(10) NOT NULL,
    sctysrs_b character varying(10) NOT NULL,
    opnpric numeric(12,2),
    hghpric numeric(12,2),
    lwpric numeric(12,2),
    clspric numeric(12,2),
    lastpric numeric(12,2),
    prvsclsgpric numeric(12,2),
    sttlmpric numeric(12,2),
    ttltradgvol bigint,
    ttltrfval bigint,
    ttlnboftxsexctd bigint,
    qty_del bigint,
    delvry_trnovr bigint,
    ttltradgvol_b bigint,
    ttltrfval_b bigint,
    ttlnboftxsexctd_b bigint,
    qty_del_b bigint,
    delvry_trnovr_b bigint,
    sum_ttltradgvol bigint,
    sum_ttltrfval bigint,
    sum_ttlnboftxsexctd bigint,
    sum_del_qty bigint,
    sum_delvry_trnovr bigint,
    del_per numeric(12,2),
    avg_price numeric(12,2),
    avg_qty_per_order bigint,
    avg_order_price numeric(12,2),
    close_to_avg numeric(12,2),
    close_avg_perc numeric(12,2),
    othr_trds character varying(20),
    othr_trds_vol bigint,
    othr_trds_val bigint,
    othr_trds_txsexctd bigint
);





CREATE TABLE public.my_daily_data_q3_2024 (
    isin character varying(20) NOT NULL,
    traddt date NOT NULL,
    sctysrs character varying(10) NOT NULL,
    sctysrs_b character varying(10) NOT NULL,
    opnpric numeric(12,2),
    hghpric numeric(12,2),
    lwpric numeric(12,2),
    clspric numeric(12,2),
    lastpric numeric(12,2),
    prvsclsgpric numeric(12,2),
    sttlmpric numeric(12,2),
    ttltradgvol bigint,
    ttltrfval bigint,
    ttlnboftxsexctd bigint,
    qty_del bigint,
    delvry_trnovr bigint,
    ttltradgvol_b bigint,
    ttltrfval_b bigint,
    ttlnboftxsexctd_b bigint,
    qty_del_b bigint,
    delvry_trnovr_b bigint,
    sum_ttltradgvol bigint,
    sum_ttltrfval bigint,
    sum_ttlnboftxsexctd bigint,
    sum_del_qty bigint,
    sum_delvry_trnovr bigint,
    del_per numeric(12,2),
    avg_price numeric(12,2),
    avg_qty_per_order bigint,
    avg_order_price numeric(12,2),
    close_to_avg numeric(12,2),
    close_avg_perc numeric(12,2),
    othr_trds character varying(20),
    othr_trds_vol bigint,
    othr_trds_val bigint,
    othr_trds_txsexctd bigint
);





CREATE TABLE public.my_daily_data_q3_2025 (
    isin character varying(20) NOT NULL,
    traddt date NOT NULL,
    sctysrs character varying(10) NOT NULL,
    sctysrs_b character varying(10) NOT NULL,
    opnpric numeric(12,2),
    hghpric numeric(12,2),
    lwpric numeric(12,2),
    clspric numeric(12,2),
    lastpric numeric(12,2),
    prvsclsgpric numeric(12,2),
    sttlmpric numeric(12,2),
    ttltradgvol bigint,
    ttltrfval bigint,
    ttlnboftxsexctd bigint,
    qty_del bigint,
    delvry_trnovr bigint,
    ttltradgvol_b bigint,
    ttltrfval_b bigint,
    ttlnboftxsexctd_b bigint,
    qty_del_b bigint,
    delvry_trnovr_b bigint,
    sum_ttltradgvol bigint,
    sum_ttltrfval bigint,
    sum_ttlnboftxsexctd bigint,
    sum_del_qty bigint,
    sum_delvry_trnovr bigint,
    del_per numeric(12,2),
    avg_price numeric(12,2),
    avg_qty_per_order bigint,
    avg_order_price numeric(12,2),
    close_to_avg numeric(12,2),
    close_avg_perc numeric(12,2),
    othr_trds character varying(20),
    othr_trds_vol bigint,
    othr_trds_val bigint,
    othr_trds_txsexctd bigint
);





CREATE TABLE public.my_daily_data_q4_2024 (
    isin character varying(20) NOT NULL,
    traddt date NOT NULL,
    sctysrs character varying(10) NOT NULL,
    sctysrs_b character varying(10) NOT NULL,
    opnpric numeric(12,2),
    hghpric numeric(12,2),
    lwpric numeric(12,2),
    clspric numeric(12,2),
    lastpric numeric(12,2),
    prvsclsgpric numeric(12,2),
    sttlmpric numeric(12,2),
    ttltradgvol bigint,
    ttltrfval bigint,
    ttlnboftxsexctd bigint,
    qty_del bigint,
    delvry_trnovr bigint,
    ttltradgvol_b bigint,
    ttltrfval_b bigint,
    ttlnboftxsexctd_b bigint,
    qty_del_b bigint,
    delvry_trnovr_b bigint,
    sum_ttltradgvol bigint,
    sum_ttltrfval bigint,
    sum_ttlnboftxsexctd bigint,
    sum_del_qty bigint,
    sum_delvry_trnovr bigint,
    del_per numeric(12,2),
    avg_price numeric(12,2),
    avg_qty_per_order bigint,
    avg_order_price numeric(12,2),
    close_to_avg numeric(12,2),
    close_avg_perc numeric(12,2),
    othr_trds character varying(20),
    othr_trds_vol bigint,
    othr_trds_val bigint,
    othr_trds_txsexctd bigint
);





CREATE TABLE public.my_daily_data_q4_2025 (
    isin character varying(20) NOT NULL,
    traddt date NOT NULL,
    sctysrs character varying(10) NOT NULL,
    sctysrs_b character varying(10) NOT NULL,
    opnpric numeric(12,2),
    hghpric numeric(12,2),
    lwpric numeric(12,2),
    clspric numeric(12,2),
    lastpric numeric(12,2),
    prvsclsgpric numeric(12,2),
    sttlmpric numeric(12,2),
    ttltradgvol bigint,
    ttltrfval bigint,
    ttlnboftxsexctd bigint,
    qty_del bigint,
    delvry_trnovr bigint,
    ttltradgvol_b bigint,
    ttltrfval_b bigint,
    ttlnboftxsexctd_b bigint,
    qty_del_b bigint,
    delvry_trnovr_b bigint,
    sum_ttltradgvol bigint,
    sum_ttltrfval bigint,
    sum_ttlnboftxsexctd bigint,
    sum_del_qty bigint,
    sum_delvry_trnovr bigint,
    del_per numeric(12,2),
    avg_price numeric(12,2),
    avg_qty_per_order bigint,
    avg_order_price numeric(12,2),
    close_to_avg numeric(12,2),
    close_avg_perc numeric(12,2),
    othr_trds character varying(20),
    othr_trds_vol bigint,
    othr_trds_val bigint,
    othr_trds_txsexctd bigint
);





CREATE TABLE public.my_static_data (
    static_id integer NOT NULL,
    isin character varying(20) NOT NULL,
    fininstrmid character varying(10),
    fininstrmid_b character varying(10),
    tckrsymb character varying(20),
    tckrsymb_b character varying(20),
    src character varying(10),
    src_b character varying(10),
    fininstrmnm character varying(50)
);





CREATE SEQUENCE public.my_static_data_static_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.my_static_data_static_id_seq OWNER TO postgres;


ALTER SEQUENCE public.my_static_data_static_id_seq OWNED BY public.my_static_data.static_id;



ALTER TABLE ONLY public.my_daily_data ATTACH PARTITION public.my_daily_data_q1_2024 FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');



ALTER TABLE ONLY public.my_daily_data ATTACH PARTITION public.my_daily_data_q1_2025 FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');



ALTER TABLE ONLY public.my_daily_data ATTACH PARTITION public.my_daily_data_q2_2024 FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');



ALTER TABLE ONLY public.my_daily_data ATTACH PARTITION public.my_daily_data_q2_2025 FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');



ALTER TABLE ONLY public.my_daily_data ATTACH PARTITION public.my_daily_data_q3_2024 FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');



ALTER TABLE ONLY public.my_daily_data ATTACH PARTITION public.my_daily_data_q3_2025 FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');



ALTER TABLE ONLY public.my_daily_data ATTACH PARTITION public.my_daily_data_q4_2024 FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');



ALTER TABLE ONLY public.my_daily_data ATTACH PARTITION public.my_daily_data_q4_2025 FOR VALUES FROM ('2025-10-01') TO ('2026-01-01');



ALTER TABLE ONLY public.financial_year ALTER COLUMN id SET DEFAULT nextval('public.financial_year_id_seq'::regclass);



ALTER TABLE ONLY public.financials_qtr ALTER COLUMN id SET DEFAULT nextval('public.financials_qtr_id_seq'::regclass);



ALTER TABLE ONLY public.my_static_data ALTER COLUMN static_id SET DEFAULT nextval('public.my_static_data_static_id_seq'::regclass);



ALTER TABLE ONLY public.financial_year
    ADD CONSTRAINT financial_year_pkey PRIMARY KEY (id);



ALTER TABLE ONLY public.financial_year
    ADD CONSTRAINT financial_year_static_id_fin_year_key UNIQUE (static_id, fin_year);



ALTER TABLE ONLY public.financials_qtr
    ADD CONSTRAINT financials_qtr_pkey PRIMARY KEY (id);



ALTER TABLE ONLY public.financials_qtr
    ADD CONSTRAINT financials_qtr_static_id_qtr_start_date_key UNIQUE (static_id, qtr_start_date);



ALTER TABLE ONLY public.my_daily_data
    ADD CONSTRAINT my_daily_data_pkey PRIMARY KEY (isin, traddt, sctysrs, sctysrs_b);



ALTER TABLE ONLY public.my_daily_data_q1_2024
    ADD CONSTRAINT my_daily_data_q1_2024_pkey PRIMARY KEY (isin, traddt, sctysrs, sctysrs_b);



ALTER TABLE ONLY public.my_daily_data_q1_2025
    ADD CONSTRAINT my_daily_data_q1_2025_pkey PRIMARY KEY (isin, traddt, sctysrs, sctysrs_b);



ALTER TABLE ONLY public.my_daily_data_q2_2024
    ADD CONSTRAINT my_daily_data_q2_2024_pkey PRIMARY KEY (isin, traddt, sctysrs, sctysrs_b);



ALTER TABLE ONLY public.my_daily_data_q2_2025
    ADD CONSTRAINT my_daily_data_q2_2025_pkey PRIMARY KEY (isin, traddt, sctysrs, sctysrs_b);



ALTER TABLE ONLY public.my_daily_data_q3_2024
    ADD CONSTRAINT my_daily_data_q3_2024_pkey PRIMARY KEY (isin, traddt, sctysrs, sctysrs_b);



ALTER TABLE ONLY public.my_daily_data_q3_2025
    ADD CONSTRAINT my_daily_data_q3_2025_pkey PRIMARY KEY (isin, traddt, sctysrs, sctysrs_b);



ALTER TABLE ONLY public.my_daily_data_q4_2024
    ADD CONSTRAINT my_daily_data_q4_2024_pkey PRIMARY KEY (isin, traddt, sctysrs, sctysrs_b);



ALTER TABLE ONLY public.my_daily_data_q4_2025
    ADD CONSTRAINT my_daily_data_q4_2025_pkey PRIMARY KEY (isin, traddt, sctysrs, sctysrs_b);



ALTER TABLE ONLY public.my_static_data
    ADD CONSTRAINT my_static_data_isin_key UNIQUE (isin);



ALTER TABLE ONLY public.my_static_data
    ADD CONSTRAINT my_static_data_pkey PRIMARY KEY (static_id);



ALTER INDEX public.my_daily_data_pkey ATTACH PARTITION public.my_daily_data_q1_2024_pkey;



ALTER INDEX public.my_daily_data_pkey ATTACH PARTITION public.my_daily_data_q1_2025_pkey;



ALTER INDEX public.my_daily_data_pkey ATTACH PARTITION public.my_daily_data_q2_2024_pkey;



ALTER INDEX public.my_daily_data_pkey ATTACH PARTITION public.my_daily_data_q2_2025_pkey;



ALTER INDEX public.my_daily_data_pkey ATTACH PARTITION public.my_daily_data_q3_2024_pkey;



ALTER INDEX public.my_daily_data_pkey ATTACH PARTITION public.my_daily_data_q3_2025_pkey;



ALTER INDEX public.my_daily_data_pkey ATTACH PARTITION public.my_daily_data_q4_2024_pkey;



ALTER INDEX public.my_daily_data_pkey ATTACH PARTITION public.my_daily_data_q4_2025_pkey;



ALTER TABLE ONLY public.financial_year
    ADD CONSTRAINT financial_year_static_id_fkey FOREIGN KEY (static_id) REFERENCES public.my_static_data(static_id);



ALTER TABLE ONLY public.financials_qtr
    ADD CONSTRAINT financials_qtr_static_id_fkey FOREIGN KEY (static_id) REFERENCES public.my_static_data(static_id);



ALTER TABLE public.my_daily_data
    ADD CONSTRAINT my_daily_data_isin_fkey FOREIGN KEY (isin) REFERENCES public.my_static_data(isin) ON DELETE CASCADE;




