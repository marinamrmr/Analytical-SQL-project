use new_schema;

-- 1
-- After exploring the data now, you are required to implement a monetary model for customers’ behavior for product purchasing and segment each customer based on the below groups:
-- 1.	Champions
-- 2.	Loyal Customers 
-- 3.	Potential Loyalists 
-- 4.	Recent Customers 
-- 5.	Promising 
-- 6.	Customers Needing Attention
-- 7.	At Risk 
-- 8.	Can’t Lose Them
-- 9.	Hibernating
-- 10.	Lost
-- The customers will be grouped based on 3 main values:
-- A.	Recency (R): how recent the last transaction is (Hint: choose a reference date, which is the most recent purchase in the dataset).
-- B.	Frequency (F): how many times the customer has bought from our store.
-- C.	Monetary (M): how much each customer has paid for our products.
-- Please give each customer scores (each from 1 to 5) based on the above three criteria, then use these resulting scores to categorize each customer into one of the above groups.
-- As there are many groups for each of the R, F, and M features, there are also many potential permutations, this number is too much to manage in terms of marketing strategies. For this, we would decrease the permutations by getting the average scores of the frequency and monetary (as both are indicative to purchase volume anyway). Use the table below as a reference to group the customers: For example, the customer with recency score = 5 and the average of (frequency and monetary) = 2 is classified as potential loyalist customer.

with rfm as (
select  distinct `customer id`, max(STR_TO_DATE(InvoiceDate ,'%m/%d/%Y %h:%i:%s %p')) as Recency,
count(distinct Invoice)as Frequency,
sum(Quantity *Price) as Monetary
from sales
group by `customer id`),

scores as(
select `customer id`,
ntile(5) over(order by Recency) as Recency_score,
ntile(5) over(order by Frequency) as Frequency_score,
ntile(5) over(order by Monetary) as Monetary_score
from rfm
order by `customer id`),

average as(
select Recency_score, round((Frequency_score + Monetary_score)/2 )as AvgFMScore
from scores)

SELECT
    CASE
        WHEN Recency_score = 5 AND AvgFMScore = 5 THEN 'Champions'
        WHEN Recency_score = 5 AND AvgFMScore = 4 THEN 'Champions'
        WHEN Recency_score = 4 AND AvgFMScore = 5 THEN 'Champions'
        
        WHEN Recency_score = 5 AND AvgFMScore = 2 THEN 'Potential Loyalists'
        WHEN Recency_score = 4 AND AvgFMScore = 2 THEN 'Potential Loyalists'
        WHEN Recency_score = 3 AND AvgFMScore = 3 THEN 'Potential Loyalists'
        WHEN Recency_score = 4 AND AvgFMScore = 3 THEN 'Potential Loyalists'

        WHEN Recency_score = 5 AND AvgFMScore = 3 THEN 'Loyal Customers'
        WHEN Recency_score = 4 AND AvgFMScore = 4 THEN 'Loyal Customers'
        WHEN Recency_score = 3 AND AvgFMScore = 5 THEN 'Loyal Customers'
        WHEN Recency_score = 3 AND AvgFMScore = 4 THEN 'Loyal Customers'

        WHEN Recency_score = 5 AND AvgFMScore = 1 THEN 'Recent Customers'

        WHEN Recency_score = 4 AND AvgFMScore = 1 THEN 'Promising'
        WHEN Recency_score = 3 AND AvgFMScore = 1 THEN 'Promising'

        WHEN Recency_score = 3 AND AvgFMScore = 2 THEN 'Customers Needing Attention'
        WHEN Recency_score = 2 AND AvgFMScore = 3 THEN 'Customers Needing Attention'
        WHEN Recency_score = 2 AND AvgFMScore = 2 THEN 'Customers Needing Attention'

        WHEN Recency_score = 2 AND AvgFMScore = 5 THEN 'At Risk'
        WHEN Recency_score = 2 AND AvgFMScore = 4 THEN 'At Risk'
        WHEN Recency_score = 2 AND AvgFMScore = 3 THEN 'At Risk'

        WHEN Recency_score = 1 AND AvgFMScore = 5 THEN 'Cant Lose Them'
        WHEN Recency_score = 1 AND AvgFMScore = 4 THEN 'Cant Lose Them'

        WHEN Recency_score = 1 AND AvgFMScore = 3 THEN 'Hibernating'
        WHEN Recency_score = 1 AND AvgFMScore = 2 THEN 'Hibernating'

        WHEN Recency_score = 1 AND AvgFMScore = 1 THEN 'Lost'
        WHEN Recency_score = 2 AND AvgFMScore = 1 THEN 'Lost'
        
    END AS group_name,
     Recency_score,
    AvgFMScore
FROM average
order by group_name;

-- 2 Identify customers who made more purchases than the average 

with customerdata as(
select distinct  `Customer ID`,sum(Price*Quantity)
over(partition by `Customer ID`)  as total_sales
from sales 
)
select *from customerdata
where total_sales > (select avg(Price*Quantity) from sales );

-- 3 Determine the average time gap between purchases for each customer
with date_data as (
select STR_TO_DATE(InvoiceDate ,'%m/%d/%Y %h:%i:%s %p') as date, `customer id`
 from sales),
 date_data2 as( select `customer id` , date,
ifnull( datediff(date,lag(date)over(partition by `customer id` order by date)),0) as diff
 from date_data)
 select distinct `customer id`,avg(ifnull(diff,0)) over(partition by `customer id`) as avg_gap
 from date_data2
 order by avg_gap desc
;
-- 4 Identify invoices with revenue (QUANTITY * PRICE) above the 90th percentile

 with rev_data as(select  Invoice ,Price *Quantity as revenue 
 from sales), perc_data as(
 select Invoice , percent_rank() over(order by revenue)*100 as percentile
 from rev_data)
 select *
 from perc_data
 where percentile >90;
 
 -- 5 Find products (STOCKCODE) with declining sales over time (month over month).
WITH quan_data AS (
    SELECT StockCode, MONTH(STR_TO_DATE(InvoiceDate, '%Y-%m-%d')) AS month,
        SUM(Quantity) AS total_quantity,  
        LEAD(SUM(Quantity)) OVER (PARTITION BY StockCode ORDER BY MONTH(STR_TO_DATE(InvoiceDate, '%Y-%m-%d'))) AS next_month_quantity
    FROM sales
    GROUP BY StockCode, MONTH(STR_TO_DATE(InvoiceDate, '%Y-%m-%d'))
)
SELECT 
    StockCode,  
    total_quantity, 
    next_month_quantity
FROM quan_data
WHERE total_quantity > next_month_quantity;


-- 6 Rank products by their popularity (by total quantity sold) 
with quan_data as(select  `Customer ID`,StockCode,Quantity, sum(Quantity) over(partition by `Customer ID`,StockCode) as total_quan
from sales
order by `Customer ID`,StockCode)
select distinct `Customer ID`,StockCode,total_quan, rank()over(partition by `Customer ID` order by Quantity desc) as ranking
from quan_data;


-- 7 Find the first, last, and third most recent purchase for each customer
-- with first_rank as(
-- select  *, dense_rank()over(partition by `Customer ID`order by STR_TO_DATE(InvoiceDate ,'%m/%d/%Y %h:%i:%s %p')) as ranking
-- from sales
-- order by `customer id`, STR_TO_DATE(InvoiceDate ,'%m/%d/%Y %h:%i:%s %p')), 

-- last_rank as(select *, dense_rank()over(partition by `Customer ID`order by STR_TO_DATE(InvoiceDate ,'%m/%d/%Y %h:%i:%s %p') desc) as ranking
-- from sales
-- order by `customer id`, STR_TO_DATE(InvoiceDate ,'%m/%d/%Y %h:%i:%s %p'))
-- select * from first_rank where ranking in(1,3)
-- union 
-- select * from last_rank where ranking = 1;


WITH aggregated_sales AS (
    SELECT 
        `Customer ID`, 
        Invoice,
        SUM(Quantity) AS total_quantity, 
        SUM(Quantity * Price) AS total_amount,
        MIN(InvoiceDate) AS first_invoice_date,
        MAX(InvoiceDate) AS last_invoice_date,
        DENSE_RANK() OVER (PARTITION BY `Customer ID` ORDER BY STR_TO_DATE(InvoiceDate, '%m/%d/%Y %h:%i:%s %p')) AS ranking_asc,
        DENSE_RANK() OVER (PARTITION BY `Customer ID` ORDER BY STR_TO_DATE(InvoiceDate, '%m/%d/%Y %h:%i:%s %p') DESC) AS ranking_desc
    FROM sales
    GROUP BY `Customer ID`, Invoice, InvoiceDate
),
selected_invoices AS (
    SELECT 
        `Customer ID`, 
        Invoice,
        total_quantity,
        total_amount,
        CASE 
            WHEN ranking_asc = 1 THEN 'First'
            WHEN ranking_asc = 3 THEN 'Third'
            WHEN ranking_desc = 1 THEN 'Last'
        END AS invoice_rank
    FROM aggregated_sales
    WHERE ranking_asc IN (1, 3) OR ranking_desc = 1
)
SELECT 
    `Customer ID`, 
    Invoice, 
    total_quantity, 
    total_amount, 
    invoice_rank
FROM selected_invoices
ORDER BY `Customer ID`, invoice_rank;
