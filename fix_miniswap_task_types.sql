-- Check existing composite types in tasks schema
DO $$
DECLARE
    rec RECORD;
    field_count INT;
BEGIN
    RAISE NOTICE 'Checking existing composite types in tasks schema...';
    
    FOR rec IN 
        SELECT t.typname, t.oid, t.typrelid
        FROM pg_type t
        WHERE t.typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'tasks')
          AND t.typtype = 'c'
        ORDER BY t.typname
    LOOP
        SELECT COUNT(*) INTO field_count
        FROM pg_attribute
        WHERE attrelid = rec.typrelid
          AND attnum > 0
          AND NOT attisdropped;
          
        RAISE NOTICE 'Type: % (OID: %, typrelid: %) has % fields', 
                     rec.typname, rec.oid, rec.typrelid, field_count;
    END LOOP;
END $$;

-- Drop and recreate the types with fields if they have no fields
-- Note: Only run this if the types have no fields!

-- Example of how to recreate types with fields:
/*
DROP TYPE IF EXISTS tasks.shipment_created CASCADE;
CREATE TYPE tasks.shipment_created AS (
    shipment_id uuid,
    seller_id uuid,
    buyer_id uuid,
    created_at timestamp with time zone
);

DROP TYPE IF EXISTS tasks.tracking_updated CASCADE;
CREATE TYPE tasks.tracking_updated AS (
    shipment_id uuid,
    tracking_number text,
    carrier text,
    status text,
    updated_at timestamp with time zone
);

DROP TYPE IF EXISTS tasks.payment_capture_required CASCADE;
CREATE TYPE tasks.payment_capture_required AS (
    payment_intent_id text,
    amount numeric(10,2),
    currency text,
    shipment_id uuid
);

DROP TYPE IF EXISTS tasks.send_password_reset CASCADE;
CREATE TYPE tasks.send_password_reset AS (
    user_id uuid,
    email text,
    token text,
    expires_at timestamp with time zone
);

DROP TYPE IF EXISTS tasks.create_stripe_customer CASCADE;
CREATE TYPE tasks.create_stripe_customer AS (
    user_id uuid,
    email text,
    name text
);

DROP TYPE IF EXISTS tasks.send_verification_code CASCADE;
CREATE TYPE tasks.send_verification_code AS (
    user_id uuid,
    email text,
    code text,
    expires_at timestamp with time zone
);

DROP TYPE IF EXISTS tasks.shipment_label_refunded CASCADE;
CREATE TYPE tasks.shipment_label_refunded AS (
    shipment_id uuid,
    refund_amount numeric(10,2),
    reason text
);

DROP TYPE IF EXISTS tasks.send_welcome_email CASCADE;
CREATE TYPE tasks.send_welcome_email AS (
    user_id uuid,
    email text,
    name text
);

DROP TYPE IF EXISTS tasks.resolve_seller_suspension CASCADE;
CREATE TYPE tasks.resolve_seller_suspension AS (
    seller_id uuid,
    suspension_reason text,
    resolved_at timestamp with time zone
);

DROP TYPE IF EXISTS tasks.shipment_cancellation_created CASCADE;
CREATE TYPE tasks.shipment_cancellation_created AS (
    shipment_id uuid,
    cancellation_id uuid,
    reason text,
    created_at timestamp with time zone
);

DROP TYPE IF EXISTS tasks.check_shipment_suspension CASCADE;
CREATE TYPE tasks.check_shipment_suspension AS (
    shipment_id uuid,
    seller_id uuid,
    check_reason text
);
*/