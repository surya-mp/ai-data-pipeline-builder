CREATE TABLE IF NOT EXISTS customers (
  customer_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  country TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
  order_id INTEGER PRIMARY KEY,
  customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
  amount NUMERIC(12,2) NOT NULL,
  ordered_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS payments (
  payment_id INTEGER PRIMARY KEY,
  order_id INTEGER NOT NULL REFERENCES orders(order_id),
  status TEXT NOT NULL,
  paid_amount NUMERIC(12,2) NOT NULL,
  paid_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO customers (customer_id, name, country) VALUES
  (1, 'Ada Lovelace', 'GB'),
  (2, 'Grace Hopper', 'US'),
  (3, 'Katherine Johnson', 'US')
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO orders (order_id, customer_id, amount) VALUES
  (101, 1, 125.00), (102, 1, 75.00), (103, 2, 250.00)
ON CONFLICT (order_id) DO NOTHING;

INSERT INTO payments (payment_id, order_id, status, paid_amount) VALUES
  (1001, 101, 'paid', 125.00), (1002, 102, 'paid', 75.00), (1003, 103, 'paid', 250.00)
ON CONFLICT (payment_id) DO NOTHING;
