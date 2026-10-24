-- ---------------------------------------------------------------------------
-- Deterministic seed data, loaded on first Postgres initialisation.
--
-- This runs BEFORE the Debezium connector is registered, so these rows are
-- emitted by Debezium's initial snapshot as SNAPSHOT_READ events. That is the
-- input for snapshot-versus-WAL precedence, which is why this
-- file is kept separate from infra/fixtures/orders_scenarios.sql.
--
-- Fixed values make the initial snapshot reproducible.
-- ---------------------------------------------------------------------------

TRUNCATE TABLE public.orders;

INSERT INTO public.orders (order_id, customer_id, status, total_amount, currency, updated_at)
VALUES
    (1001, 501, 'NEW',    120.50, 'EUR', TIMESTAMPTZ '2026-01-01 10:00:00+00'),
    (1002, 502, 'PAID',    340.00, 'EUR', TIMESTAMPTZ '2026-01-01 10:00:10+00'),
    (1003, 503, 'NEW',     75.25, 'EUR', TIMESTAMPTZ '2026-01-01 10:00:20+00'),
    (1004, 501, 'SHIPPED', 210.00, 'EUR', TIMESTAMPTZ '2026-01-01 10:00:30+00'),
    (1005, 504, 'CANCELLED', 99.99, 'EUR', TIMESTAMPTZ '2026-01-01 10:00:40+00');
