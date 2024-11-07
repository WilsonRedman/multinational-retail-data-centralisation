--Altering data types, applied to other tables according to schema
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID
ALTER COLUMN user_uuid TYPE UUID
ALTER COLUMN card_number TYPE VARCHAR(19)
ALTER COLUMN store_code TYPE VARCHAR(12)
ALTER COLUMN product_code TYPE VARCHAR(11)
ALTER COLUMN product_quantity TYPE SMALLINT



--Query to create a new column based on weight values
ALTER TABLE dim_products
ADD weight_class GENERATED ALWAYS AS(
CASE    
    WHEN (weight < (2)::numeric) THEN 'Light'::text    
    WHEN (weight < (40)::numeric) THEN 'Mid_Sized'::text    
    WHEN (weight < (140)::numeric) THEN 'HEAVY'::text    ELSE 'Truck_Required'::text
END 
) STORED



--Primary key creation, applied to other tables as well
ALTER TABLE dim_card_details
ADD PRIMARY KEY(card_number)



--Foreign key creation, all on orders_table to create star based schema
ALTER TABLE orders_table
ADD FOREIGN KEY(date_uuid) REFERENCES dim_date_times(date_uuid),
ADD FOREIGN KEY(user_uuid) REFERENCES dim_users(user_uuid),
ADD FOREIGN KEY(card_number) REFERENCES dim_card_details(card_number),
ADD FOREIGN KEY(store_code) REFERENCES dim_store_details(store_code),
ADD FOREIGN KEY(product_code) REFERENCES dim_products(product_code)