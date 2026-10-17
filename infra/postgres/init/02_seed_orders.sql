-- ---------------------------------------------------------------------------
-- Deterministic seed data, loaded on first Postgres initialisation.
--
-- This runs BEFORE the Debezium connector is registered, so these rows are
-- emitted by Debezium's initial snapshot as SNAPSHOT_READ events. That is the
-- input for the snapshot-vs-CDC precedence rule (SRS FR-010) and it is why this
-- file is kept separate from infra/fixtures/orders_scenarios.sql.
--
-- Values are fixed and explicit (ids, timestamps and amounts) so the same run
-- always produces the same set of events and therefore the same expected state
-- (SRS FR-018, AC-01).
-- ---------------------------------------------------------------------------

TRUNCATE TABLE public.orders;

INSERT INTO public.orders (order_id, customer_id, status, total_amount, currency, updated_at)
VALUES
    (1001, 501, 'NEW',    120.50, 'EUR', TIMESTAMPTZ '2026-01-01 10:00:00+00'),
    (1002, 502, 'PAID',    340.00, 'EUR', TIMESTAMPTZ '2026-01-01 10:00:10+00'),
    (1003, 503, 'NEW',     75.25, 'EUR', TIMESTAMPTZ '2026-01-01 10:00:20+00'),
    (1004, 501, 'SHIPPED', 210.00, 'EUR', TIMESTAMPTZ '2026-01-01 10:00:30+00'),
    (1005, 504, 'CANCELLED', 99.99, 'EUR', TIMESTAMPTZ '2026-01-01 10:00:40+00');