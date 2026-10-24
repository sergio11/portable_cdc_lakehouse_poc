-- ---------------------------------------------------------------------------
-- Deterministic CDC scenarios.
--
-- Applied on demand with `rake fixtures:scenarios`, AFTER the Debezium connector
-- is registered, so every statement below is captured as an ordinary WAL change
-- (CREATE / UPDATE / DELETE) rather than part of the initial snapshot.
--
-- Expected outcome per key once the history is materialised:
--
--   order_id  scenario                          expected final state
--   --------  -------------------------------  -------------------------------
--   1001      UPDATE status -> PAID            live, status=PAID
--   1002      UPDATE total_amount -> 349.00    live, total_amount=349.00
--   1003      no change                        live, untouched
--   1004      DELETE                           is_deleted=true (tombstone)
--   1005      DELETE then re-INSERT            live (delete does not block a newer create)
--   1006      INSERT                            live, NEW
--   1007      INSERT then UPDATE                live, SHIPPED
--
-- Every mutation carries an explicit updated_at so that the projection's
-- updated_at ordering is reproducible across runs. The pause between statements
-- gives Debezium a commit boundary per scenario, which keeps the WAL ordering
-- unambiguous; ordering semantics do not rely on it (LSN decides), it only makes
-- the generated event stream easier to reason about while debugging.
-- ---------------------------------------------------------------------------

BEGIN;

-- 1001: ordinary update.
UPDATE public.orders
   SET status = 'PAID', updated_at = TIMESTAMPTZ '2026-01-02 09:00:00+00'
 WHERE order_id = 1001;

-- 1002: update of a non-key business column.
UPDATE public.orders
   SET total_amount = 349.00, updated_at = TIMESTAMPTZ '2026-01-02 09:00:10+00'
 WHERE order_id = 1002;

-- 1004: delete -> must become a versioned tombstone.
DELETE FROM public.orders WHERE order_id = 1004;

-- 1005: delete followed by a newer re-insert of the same key. The final state
-- must be live, because the create carries a strictly newer source position than
-- the delete.
DELETE FROM public.orders WHERE order_id = 1005;
INSERT INTO public.orders (order_id, customer_id, status, total_amount, currency, updated_at)
VALUES (1005, 504, 'NEW', 99.99, 'EUR', TIMESTAMPTZ '2026-01-02 09:00:30+00');

-- 1006: pure insert.
INSERT INTO public.orders (order_id, customer_id, status, total_amount, currency, updated_at)
VALUES (1006, 505, 'NEW', 15.00, 'EUR', TIMESTAMPTZ '2026-01-02 09:00:40+00');

-- 1007: insert then update in the same transaction.
INSERT INTO public.orders (order_id, customer_id, status, total_amount, currency, updated_at)
VALUES (1007, 506, 'NEW', 60.00, 'EUR', TIMESTAMPTZ '2026-01-02 09:00:50+00');
UPDATE public.orders
   SET status = 'SHIPPED', updated_at = TIMESTAMPTZ '2026-01-02 09:00:55+00'
 WHERE order_id = 1007;

COMMIT;
