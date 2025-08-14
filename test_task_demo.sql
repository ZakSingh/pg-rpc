-- Test demo for task types with bulk @pgrpc_not_null annotations

-- Create the task schema
CREATE SCHEMA IF NOT EXISTS tasks;

-- Create a composite type with dollar-quoted comment containing bulk not null annotation
DROP TYPE IF EXISTS tasks.create_authorization CASCADE;

CREATE TYPE tasks.create_authorization AS (
    payment_method_id text,
    stripe_customer_id text,
    stripe_account_id text,
    checkout_id text,
    seller_id text,
    order_nanoid text,
    buyer_total numeric,
    item_subtotal numeric,
    shipping_subtotal numeric,
    seller_fee numeric,
    optional_field text
);

-- Add comment with bulk not null annotation using dollar quotes
COMMENT ON TYPE tasks.create_authorization IS $$ Task payload for creating payment authorization when setup intent succeeds.
This type includes all necessary information for payment processing.

@pgrpc_not_null(payment_method_id, stripe_customer_id, stripe_account_id, checkout_id, seller_id, order_nanoid, buyer_total, item_subtotal, shipping_subtotal, seller_fee)
$$;

-- Create another task type with annotations on newlines
DROP TYPE IF EXISTS tasks.send_notification CASCADE;

CREATE TYPE tasks.send_notification AS (
    user_id text,
    channel text,
    subject text,
    body text,
    priority text,
    metadata jsonb
);

COMMENT ON TYPE tasks.send_notification IS $$ Task for sending notifications to users.

@pgrpc_not_null(
    user_id,
    channel,
    subject,
    body
)
$$;