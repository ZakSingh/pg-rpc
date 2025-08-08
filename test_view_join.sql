CREATE TABLE address (
    address_id int PRIMARY KEY,
    line1 text NOT NULL,
    city text NOT NULL
);

CREATE TABLE account_delivery_address (
    account_id int,
    address_id int,
    delivery_instructions text,
    recipient_name text NOT NULL,
    deleted_at timestamp
);

CREATE TABLE account (
    account_id int PRIMARY KEY,
    default_delivery_address_id int
);

-- Test view with JOIN and whole-row reference
CREATE VIEW delivery_address_test AS
SELECT ada.account_id,
       a.address_id,
       ada.delivery_instructions,
       ada.recipient_name,
       a AS address,  -- whole row reference
       ada.deleted_at,
       (ac.default_delivery_address_id = a.address_id) AS is_default
FROM account_delivery_address ada
JOIN address a USING (address_id)
JOIN account ac USING (account_id);
EOF < /dev/null