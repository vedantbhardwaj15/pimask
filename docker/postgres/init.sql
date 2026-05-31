-- Create tables (applicable to both production_raw and testing_clean)
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id) ON DELETE CASCADE,
    amount DECIMAL(10, 2) NOT NULL,
    credit_card VARCHAR(20) NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed high-volume production data only into production_raw database
DO $$
BEGIN
    IF current_database() = 'production_raw' THEN
        -- Verify if already seeded to prevent duplicate keys
        IF NOT EXISTS (SELECT 1 FROM users) THEN
            -- Generate 10,000 users
            INSERT INTO users (name, email, phone)
            SELECT 
                'User ' || i,
                'user_' || i || '@example.com',
                '+1-555-' || lpad(i::text, 4, '0')
            FROM generate_series(1, 10000) AS i;

            -- Generate 15,000 orders referencing those users
            INSERT INTO orders (user_id, amount, credit_card)
            SELECT 
                (floor(random() * 10000) + 1)::int,
                (random() * 500 + 5)::numeric(10,2),
                lpad(floor(random() * 10000)::text, 4, '0') || '-' || 
                lpad(floor(random() * 10000)::text, 4, '0') || '-' || 
                lpad(floor(random() * 10000)::text, 4, '0') || '-' || 
                lpad(floor(random() * 10000)::text, 4, '0')
            FROM generate_series(1, 15000) AS i;
        END IF;
    END IF;
END $$;
