/* 
 Create temporary table
 
 This is the table we want to join other data on. We only need to apply some filter on top of it. For example a seller_id filter.
 */
CREATE TEMPORARY TABLE IF NOT EXISTS order_subset
SELECT *
FROM orders
    JOIN(
        SELECT *
        FROM order_items
            JOIN (
                SELECT product_id,
                    product_category_name_english
                FROM products
                    JOIN product_category_name_translation USING(product_category_name)
                WHERE product_category_name_english IN (
                        'electronics',
                        'computers_accessories',
                        'computers',
                        'tablets_printing_image',
                        'telephony'
                    )
            ) AS ProCat USING(product_id)
    ) AS ProCatPri USING(order_id)
WHERE NOT order_status IN ('unavailable', 'canceled');
/* 
 Get the relevant list of sellers
 */
CREATE TEMPORARY TABLE IF NOT EXISTS relevant_sellers_id
SELECT seller_id
FROM (
        SELECT seller_id,
            AVG(total_price + total_freight_value) AS 'avg_order_price',
            AVG((total_price / total_products_in_order)) AS 'avg_product_price'
        FROM (
                SELECT order_id,
                    customer_id,
                    seller_id,
                    CONVERT(AVG(order_purchase_timestamp), DATETIME) AS 'order_purchase_timestamp',
                    CONVERT(AVG(order_delivered_customer_date), DATETIME) AS 'order_delivered_customer_date',
                    CONVERT(AVG(order_estimated_delivery_date), DATETIME) AS 'order_estimated_delivery_date',
                    MAX(order_item_id) AS 'total_products_in_order',
                    SUM(price) AS 'total_price',
                    SUM(freight_value) AS 'total_freight_value'
                FROM order_subset
                GROUP BY order_id,
                    customer_id,
                    seller_id
            ) AS order_customer_seller
        GROUP BY seller_id
    ) AS S
WHERE avg_order_price >= 600
    AND avg_product_price >= 500;
/* 
 this list can then be used on the first table order_subset to subset the relevant data from it. For example, get all order_ids
 */
 
 /* delivery time per seller and order id */
SELECT 
	AVG(TIMESTAMPDIFF(DAY, order_purchase_timestamp, order_delivered_customer_date)) AS delivery_time,
	seller_id,
    order_id,
	CONVERT(AVG(order_purchase_timestamp), DATETIME) AS 'order_purchase_timestamp',
	CONVERT(AVG(order_delivered_customer_date), DATETIME) AS 'order_delivered_customer_date',
	CONVERT(AVG(order_estimated_delivery_date), DATETIME) AS 'order_estimated_delivery_date'
FROM
    (SELECT 
        *
    FROM
        order_subset
    WHERE
        seller_id IN (SELECT 
                *
            FROM
                relevant_sellers_id)) AS order_table
GROUP BY seller_id, order_id;
/*===============================================================================================*/


/* average delivery time from the subset (without NULL values in order time) */
SELECT 
    AVG(avg_delivery_time_day) AS 'avg_delivery_time_subset'
FROM
    (SELECT 
        seller_id,
            ROUND(AVG(delivery_time_hr) / 24, 1) AS avg_delivery_time_day,
            AVG(total_price + total_freight_value) AS 'avg_order_price',
            AVG((total_price / total_products_in_order)) AS 'avg_product_price',
            COUNT(DISTINCT order_id) AS 'number_of_orders'
    FROM
        (SELECT 
        order_id,
            customer_id,
            seller_id,
            CONVERT( AVG(order_purchase_timestamp) , DATETIME) AS 'order_purchase_timestamp',
            CONVERT( AVG(order_delivered_customer_date) , DATETIME) AS 'order_delivered_customer_date',
            CONVERT( AVG(order_estimated_delivery_date) , DATETIME) AS 'order_estimated_delivery_date',
            TIMESTAMPDIFF(HOUR, AVG(order_purchase_timestamp), AVG(order_delivered_customer_date)) AS 'delivery_time_hr',
            MAX(order_item_id) AS 'total_products_in_order',
            SUM(price) AS 'total_price',
            SUM(freight_value) AS 'total_freight_value'
    FROM
        order_subset
    WHERE
        NOT order_purchase_timestamp IS NULL
            AND NOT order_delivered_customer_date IS NULL
    GROUP BY order_id , customer_id , seller_id) AS order_customer_seller
    GROUP BY seller_id) AS S
WHERE
    avg_order_price >= 600
        AND avg_product_price >= 500;
/*===============================================================================================*/        


/* average delivery time from the subset, with number of order >= 10 (without NULL values in order time) */        
SELECT 
    AVG(avg_delivery_time_day) AS 'avg_delivery_time_subset_top5'
FROM
    (SELECT 
        seller_id,
            ROUND(AVG(delivery_time_hr) / 24, 1) AS avg_delivery_time_day,
            AVG(total_price + total_freight_value) AS 'avg_order_price',
            AVG((total_price / total_products_in_order)) AS 'avg_product_price',
            COUNT(DISTINCT order_id) AS 'number_of_orders'
    FROM
        (SELECT 
        order_id,
            customer_id,
            seller_id,
            CONVERT( AVG(order_purchase_timestamp) , DATETIME) AS 'order_purchase_timestamp',
            CONVERT( AVG(order_delivered_customer_date) , DATETIME) AS 'order_delivered_customer_date',
            CONVERT( AVG(order_estimated_delivery_date) , DATETIME) AS 'order_estimated_delivery_date',
            TIMESTAMPDIFF(HOUR, AVG(order_purchase_timestamp), AVG(order_delivered_customer_date)) AS 'delivery_time_hr',
            MAX(order_item_id) AS 'total_products_in_order',
            SUM(price) AS 'total_price',
            SUM(freight_value) AS 'total_freight_value'
    FROM
        order_subset
    WHERE
        NOT order_purchase_timestamp IS NULL
            AND NOT order_delivered_customer_date IS NULL
    GROUP BY order_id , customer_id , seller_id) AS order_customer_seller
    GROUP BY seller_id) AS S
WHERE
    avg_order_price >= 600
        AND avg_product_price >= 500
        AND number_of_orders >= 10;
/*===============================================================================================*/
    

/* Joining tables for product information's influence on delivery time */

SELECT *, ((TIMESTAMPDIFF(DAY, order_estimated_delivery_date, order_delivered_customer_date))) AS punctuality, 
		(TIMESTAMPDIFF(DAY, order_purchase_timestamp, order_delivered_customer_date)) AS delivery_time
FROM 
    order_subset
        JOIN
    products USING (product_id)
        JOIN
    customers USING (customer_id)
        JOIN
    sellers USING (seller_id)
        JOIN
    geo ON zip_code_prefix = seller_zip_code_prefix
        OR zip_code_prefix = customer_zip_code_prefix
WHERE
    seller_id IN (SELECT 
            *
        FROM
            relevant_sellers_id);
/*===============================================================================================*/

/* commands to drop the temporary tables */    
DROP TABLE order_subset;
DROP TABLE relevant_sellers_id;