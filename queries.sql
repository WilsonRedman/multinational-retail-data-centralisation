--Viewing how many stores each country has
SELECT 
	country_code AS country,
	COUNT(country_code) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC



--Viewing which locations have the most stores
SELECT 
	locality,
	COUNT(locality) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC



--Viewing which months generate the most sales
SELECT 
	SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
	dim_date_times.month AS month
FROM orders_table
	INNER JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
	INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY month
ORDER BY total_sales DESC



--Viewing the number of sales online vs offline
SELECT 
	COUNT(orders_table.product_quantity) AS number_of_sales,
	SUM(orders_table.product_quantity) AS product_quantity_count,
	CASE
		WHEN dim_store_details.address = 'N/A' THEN 'Web'
		ELSE 'Offline'
	END AS location
FROM orders_table
	INNER JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY 
	CASE
		WHEN dim_store_details.address = 'N/A' THEN 'Web'
		ELSE 'Offline'
	END



--Viewing percentage of sales of each store type
SELECT
	dim_store_details.store_type AS store_type,
	SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
	ROUND(SUM(orders_table.product_quantity * dim_products.product_price) * 100 /
	SUM(SUM(orders_table.product_quantity * dim_products.product_price)) OVER(), 2) AS "sales_made(%)"
FROM orders_table
	INNER JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
	INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY store_type
ORDER BY total_sales DESC



--Viewing which month and year produced the highest sales
SELECT 
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
    dim_date_times.year AS year,
    dim_date_times.month AS month
FROM orders_table
    INNER JOIN dim_date_times on orders_table.date_uuid = dim_date_times.date_uuid
    INNER JOIN dim_products on orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.year, dim_date_times.month
ORDER BY total_sales DESC



--Viewing staff headcount across countries
SELECT SUM(staff_numbers) AS total_staff_numbers, country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC



--Viewing the German store type generating most sales
SELECT 
    SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales,
    dim_store_details.store_type AS store_type,
    dim_store_details.country_code AS country_code
FROM orders_table
    INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code
    INNER JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
WHERE dim_store_details.country_code = 'DE'
GROUP BY dim_store_details.store_type, dim_store_details.country_code



-- Viewing average time between sales across years
WITH whole_timestamp AS(
	SELECT
	year,
	TO_TIMESTAMP(year||'-'||month||'-'||day||' '||timestamp, 'YYYY-MM-DD HH24:MI:SS')::timestamp AS time
	FROM dim_date_times
),
time_diffs AS(
	SELECT
	year,
	EXTRACT(EPOCH FROM(LEAD(time) OVER(ORDER BY year, time) - time)) AS time_diff
	FROM whole_timestamp
)
SELECT
	year,
	CONCAT(
		'hours: ', FLOOR(AVG(time_diff)/3600),
		', minutes: ', FLOOR((AVG(time_diff)%3600)/60),
		', seconds: ', FLOOR((AVG(time_diff)%60)),
		', miliseconds ', FLOOR((AVG(time_diff)%1)*1000)
	)
FROM time_diffs
GROUP BY year
ORDER BY AVG(time_diff) DESC