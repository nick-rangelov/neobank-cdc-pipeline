-- Raw layer for the CDC project: append-only tables, never edited after loading.

create schema if not exists raw_cdc;

create table if not exists raw_cdc.customers_snapshot (
    customer_id   integer,
    first_name    varchar(100),
    last_name     varchar(100),
    email         varchar(255),
    country       varchar(100),
    segment       varchar(50),
    _source_file  varchar(512),
    _loaded_at    timestamp default getdate()
);

create table if not exists raw_cdc.customer_events (
    seq               bigint,
    op                char(1),
    commit_ts         timestamp,
    customer_id       integer,
    first_name        varchar(100),
    last_name         varchar(100),
    email             varchar(255),
    country           varchar(100),
    segment           varchar(50),
    marketing_opt_in  varchar(10),
    _source_file      varchar(512),
    _loaded_at        timestamp default getdate()
);

-- Which files have been loaded. Redshift does NOT enforce primary keys or unique
-- constraints, so the loader itself must check this table before loading a file.
create table if not exists raw_cdc.load_log (
    source_file  varchar(512),
    row_count    integer,
    loaded_at    timestamp default getdate()
);

-- Learning-project convenience so a script can use these tables with an IAM login.
-- In production, grant to a specific role instead of public.
grant usage, create on schema raw_cdc to public;
grant all on all tables in schema raw_cdc to public;
