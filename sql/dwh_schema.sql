-- FinanData Perú - esquema mínimo de Data Warehouse para Supabase PostgreSQL.
-- Los umbrales de calidad pertenecen a la configuración de la PoC, no a SBS.

CREATE TABLE IF NOT EXISTS dim_sucursal (
    branch_id TEXT PRIMARY KEY,
    branch_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_canal (
    channel TEXT PRIMARY KEY,
    channel_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_moneda (
    currency TEXT PRIMARY KEY,
    currency_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_transacciones (
    transaction_id TEXT PRIMARY KEY,
    transaction_date TIMESTAMPTZ NOT NULL,
    branch_id TEXT NOT NULL REFERENCES dim_sucursal(branch_id),
    channel TEXT NOT NULL REFERENCES dim_canal(channel),
    currency TEXT NOT NULL REFERENCES dim_moneda(currency),
    amount NUMERIC(18, 2) NOT NULL,
    commission NUMERIC(18, 2) NOT NULL,
    debit_amount NUMERIC(18, 2) NOT NULL,
    credit_amount NUMERIC(18, 2) NOT NULL,
    risk_score INTEGER NOT NULL,
    iban_masked TEXT NOT NULL,
    source_system TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    flow_run_id TEXT NOT NULL,
    record_hash TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_transacciones_batch_id
    ON fact_transacciones(batch_id);

CREATE TABLE IF NOT EXISTS etl_batch_control (
    batch_id TEXT PRIMARY KEY,
    flow_run_id TEXT NOT NULL,
    fecha_proceso DATE NOT NULL DEFAULT CURRENT_DATE,
    input_records INTEGER NOT NULL DEFAULT 0,
    valid_records INTEGER NOT NULL DEFAULT 0,
    rejected_records INTEGER NOT NULL DEFAULT 0,
    reject_rate NUMERIC(9, 6) NOT NULL DEFAULT 0,
    qg1_status TEXT NOT NULL DEFAULT 'NOT_EVALUATED',
    loaded_records INTEGER NOT NULL DEFAULT 0,
    qg2_status TEXT NOT NULL DEFAULT 'NOT_EVALUATED',
    pipeline_status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

INSERT INTO dim_sucursal(branch_id, branch_name) VALUES
    ('LIM-001', 'Lima Centro'),
    ('LIM-002', 'Lima Norte'),
    ('LIM-003', 'Lima Sur')
ON CONFLICT (branch_id) DO UPDATE SET branch_name = EXCLUDED.branch_name;

INSERT INTO dim_canal(channel, channel_name) VALUES
    ('ATM', 'Cajero automático'),
    ('MOBILE', 'Banca móvil'),
    ('ACH', 'Core / ACH')
ON CONFLICT (channel) DO UPDATE SET channel_name = EXCLUDED.channel_name;

INSERT INTO dim_moneda(currency, currency_name) VALUES
    ('PEN', 'Sol peruano'),
    ('USD', 'Dólar estadounidense')
ON CONFLICT (currency) DO UPDATE SET currency_name = EXCLUDED.currency_name;

