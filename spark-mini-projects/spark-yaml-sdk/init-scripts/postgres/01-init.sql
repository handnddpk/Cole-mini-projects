-- Create customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    address VARCHAR(200),
    registration_date DATE NOT NULL,
    status VARCHAR(20) DEFAULT 'active'
);

-- Insert sample customer data
INSERT INTO customers (customer_name, email, address, registration_date, status)
VALUES
    ('John Smith', 'john.smith@example.com', '123 Main St, New York, NY', '2022-01-15', 'active'),
    ('Jane Doe', 'jane.doe@example.com', '456 Oak Ave, San Francisco, CA', '2022-02-20', 'active'),
    ('Robert Johnson', 'robert.j@example.com', '789 Pine Rd, Chicago, IL', '2022-03-10', 'active'),
    ('Emily Wilson', 'emily.w@example.com', '321 Cedar Ln, Boston, MA', '2022-04-05', 'inactive'),
    ('Michael Brown', 'michael.b@example.com', '654 Maple Dr, Seattle, WA', '2022-05-12', 'active'),
    ('Sarah Miller', 'sarah.m@example.com', '987 Birch Blvd, Austin, TX', '2022-06-18', 'active'),
    ('David Garcia', 'david.g@example.com', '159 Elm St, Denver, CO', '2022-07-22', 'active'),
    ('Lisa Martinez', 'lisa.m@example.com', '753 Walnut Ave, Miami, FL', '2022-08-30', 'inactive'),
    ('James Taylor', 'james.t@example.com', '246 Cherry Ln, Portland, OR', '2022-09-14', 'active'),
    ('Jennifer Anderson', 'jennifer.a@example.com', '135 Spruce Rd, Atlanta, GA', '2022-10-25', 'active');
