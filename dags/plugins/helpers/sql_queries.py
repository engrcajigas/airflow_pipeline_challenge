class SQLQueries:
    
    products_table = 'products'
    
    users_table = 'users'
    
    time_table = 'time'
    
    product_reviews_table = 'product_reviews'
    
    products_table_create = ("""
            CREATE TABLE IF NOT EXISTS products(
                asin VARCHAR PRIMARY KEY,
                price VARCHAR,
                categories VARCHAR,
                overall_rating INT
            );
    """)
    
    users_table_create = ("""
            CREATE TABLE IF NOT EXISTS users(
                reviewerID VARCHAR PRIMARY KEY,
                reviewerName VARCHAR
            );
    """)

    time_table_create = ("""
            CREATE TABLE IF NOT EXISTS time(
                unixReviewTime TIMESTAMP,
                asin VARCHAR,
                reviewerID VARCHAR,
                year INT NOT NULL CHECK (year >= 0),
                month INT NOT NULL CHECK (month >= 0),
                day INT NOT NULL CHECK (day >= 0)
            );
    """)
    
    product_reviews_table_create = ("""
            CREATE TABLE IF NOT EXISTS product_reviews(
                reviewerID VARCHAR,
                reviewerName VARCHAR,
                asin VARCHAR,
                reviewText VARCHAR,
                summary VARCHAR,
                overall INT
            );
    """)

    products_table_insert = ("""
            INSERT INTO products (
                asin,
                price,
                categories,
                overall_rating
            ) VALUES (?, ?, ?, ?);
    """)
    
    users_table_insert = ("""
            INSERT INTO users (
                reviewerID,
                reviewerName
            ) VALUES (?, ?)
    """)

    time_table_insert = ("""
            INSERT INTO time(
                unixReviewTime,
                asin, 
                reviewerID,
                year,
                month, 
                day
            ) VALUES (?, ?, ?, ?, ?, ?);
    """)
    
    product_reviews_table_insert = ("""
            INSERT INTO product_reviews (
                reviewerID,
                reviewerName,
                asin,
                reviewText,
                summary,
                overall,
            ) VALUES (?, ?, ?, ?, ?, ?)
    """)
    
    users_table_drop = "DROP TABLE IF EXISTS users;"
    
    products_table_drop = "DROP TABLE IF EXISTS products;"
    
    time_table_drop = "DROP TABLE IF EXISTS time;"

    product_reviews_drop = "DROP TABLE IF EXISTS product_reviews"