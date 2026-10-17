-- ---------------------------------------------------------------------------
-- Operational source schema for the POC (SRS 2.1).
--
-- Scope: the MVP targets a single table, `public.orders`, with a single source
-- and a stable primary key (SRS 5.2). `customers` and `payments` are explicitly
-- out of scope for the MVP; see the scope decision in docs/architecture.md.
--
-- Loaded only on first initialisation of an empty Postgres volume.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.orders (
    order_id     BIGINT       PRIMARY KEY,
    customer_id  BIGINT       NOT NULL,
    status       TEXT         NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    currency     TEXT         NOT NULL DEFAULT 'EUR',
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT orders_status_ck CHECK (status IN ('NEW', 'PAID', 'SHIPPED', 'CANCELLED'))
);

-- Debezium captures every row change; this index only helps fixture generation.
CREATE INDEX IF NOT EXISTS orders_customer_idx ON public.orders (customer_id);

-- Deterministic replay needs a stable, documented set of business columns. The
-- column order here is also the projection order used by the Iceberg schema.
COMMENT ON TABLE public.orders IS 'MVP CDC source table for the Portable CDC Lakehouse POC';