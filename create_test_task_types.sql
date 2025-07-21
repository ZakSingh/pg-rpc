-- Create tasks schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS tasks;

-- Create some example composite types with fields
CREATE TYPE tasks.shipment_created AS (
    shipment_id uuid,
    seller_id uuid,
    created_at timestamp with time zone
);

CREATE TYPE tasks.tracking_updated AS (
    shipment_id uuid,
    tracking_number text,
    carrier text,
    status text,
    updated_at timestamp with time zone
);

CREATE TYPE tasks.payment_capture_required AS (
    payment_id uuid,
    amount numeric(10,2),
    currency text
);

CREATE TYPE tasks.send_password_reset AS (
    user_id uuid,
    email text,
    token text,
    expires_at timestamp with time zone
);

-- Show what we created
\d tasks.shipment_created
\d tasks.tracking_updated
\d tasks.payment_capture_required 
\d tasks.send_password_reset