DESCRIBE superstore_sales_dataset;

-- Added 2 new columns with date datatype
Alter TABLE superstore_sales_dataset
add column order_date_dt DATE,
add column ship_date_dt DATE;

-- Converted Str date columns to Date datatype
update  superstore_sales_dataset
set 
	order_date_dt = str_to_date(Order_Date,'%d/%m/%Y'),
    ship_date_dt = str_to_date(Ship_Date, '%d/%m/%Y');
    
select Ship_Date, ship_date_dt
from superstore_sales_dataset
limit 10;

-- Drop old column
alter table superstore_sales_dataset
drop column Order_Date,
DROP COLUMN Ship_Date;

Select * from superstore_sales_dataset;
----------------------------------------------
-- Tables Bi-furcations
-- Product Table
create table Product_table(
	Product_ID VARCHAR(100) PRIMARY key,
    Category VARCHAR(100),
    Sub_category VARCHAR(100),
    Product_name VARCHAR(1000)  
);
-- Customer Table
create table Customer_table(
	Customer_ID VARCHAR(100) PRIMARY key,
    Category_name VARCHAR(100),
    Segment VARCHAR(100),
    City VARCHAR(100),
    State VARCHAR(100),
    Country VARCHAR(100),
    Region VARCHAR(100),
    Postal_code INT
);
-- Order Table
create table Order_table(
	Order_ID VARCHAR(100) PRIMARY key,
    Customer_ID VARCHAR(100),
    Product_ID VARCHAR(100),
    Order_date DATE,
    Ship_date DATE,
    Ship_mode VARCHAR(100),
    Sales DECIMAL(10,2)    
);
select * from Product_table;
select * from Customer_table;
select * from Order_table;
------------------------------------
-- Data Insert --
-- Product_table
insert into Product_table
select
	Product_ID,
    Max(Category) as Category,
    MAX(Sub_category) as Sub_category,
    MAX(Product_name) as Product_name
from superstore_sales_dataset
GROUP BY Product_ID;

-- Customer_table
insert into Customer_table
select
	Customer_ID,
    Max(Customer_name) as Customer_name,
    Max(Segment) as Segment,
    Max(City) as City,
    Max(State) as State,
    Max(Country) as Country,
    Max(Region) as Region,
    Max(Postal_code) as Postal_code
from superstore_sales_dataset
GROUP BY Customer_ID;

-- Order_table
insert into Order_table
select
	Row_ID,
	Order_ID,
    Customer_ID as Customer_ID,
    Product_ID as Product_ID ,
    Order_date_dt as Order_date ,
    Ship_date_dt as  Ship_date ,
    Ship_mode as Ship_mode ,
    Sales as Sales 
from superstore_sales_dataset;

-- Adding Foreign key to order_table
alter table order_table
add constraint fk_ot_ct foreign key(Customer_ID) references customer_table(Customer_ID),
add constraint fk_ot_pt foreign key(Product_ID) references product_table(Product_ID);

-- verify count of data
select count(*) from product_table;
select count(*) from customer_table;
select count(*) from order_table;

-- Business Related Data Analysis ----------------------------------------------

-- 1. Sales & Revenue Performance --------------------------

-- A. How has total sales trended month-over-month and year-over-year?

SELECT year(order_date) as yr, sum(Sales) as Sales, 
	round((sum(Sales) - lag(sum(sales)) over (order by year(order_date)))*100 / 
    lag(sum(sales)) over (order by year(order_date)),2) as percent_change
from order_table
GROUP BY yr
order by yr, Sales desc;

SELECT  Year(order_date) as yr, 
		Month(order_date) as Months, 
        sum(Sales) as Sales,
        round((sum(Sales) - lag(sum(sales)) over (order by year(order_date),Month(order_date)))*100 / 
        lag(sum(sales)) over (order by year(order_date),Month(order_date)),2) as percent_change
from order_table
GROUP BY  yr, Months
order by yr, months;

-- B. Which months show peak and low sales activity?

SELECT  Year(order_date) as yr, 
		Month(order_date) as Months, 
		sum(Sales) as Sales
from order_table
GROUP BY  1,2
order by Sales;

-- C. Which regions contribute the highest share of total sales?

Select  c.Region, 
		sum(o.Sales) as Total_sales, 
        round(sum(o.Sales)*100.0 / sum(sum(o.Sales)) over(),2) as percentage_contributation 
from order_table o 
JOIN customer_table c 
on o.Customer_ID = c.Customer_ID
Group by c.Region
order by Total_sales Desc;

-- D. How does sales performance vary across categories and sub-categories?

Select p.Category, p.Sub_category,sum(o.Sales) as Total_sales
from order_table o 
JOIN product_table p 
on o.Product_ID = p.Product_ID
Group by p.Category, p.Sub_category
order by p.Category, p.Sub_category, Total_sales Desc;

-- 2. CUSTOMER ANALYTICS --------------------------

-- E. Who are the top customers by total sales value?

Select c.Customer_ID, c.Customer_name, c.City, c.State, c.Country, sum(o.Sales) as Total_sales
from order_table o 
JOIN customer_table c 
on o.Customer_ID = c.Customer_ID
Group by c.Customer_ID
order by Total_sales Desc
limit 10;

-- F. What percentage of customers are repeat customers?

select  round(count(distinct case when repeat_cust>1 then Customer_ID end) *100.0 / COUNT( distinct Customer_ID),2) as pertage
from (  select Customer_ID, count(*) as repeat_cust
		from order_table
		GROUP BY Customer_ID) as t;

-- G. How frequently do customers place orders?

select  avg(order_count) as freq
from (  
		select Customer_ID, count(*) as order_count
		from order_table
		GROUP BY Customer_ID) as t2;

-- H. How long does a typical customer remain active after their first purchase?

select  Customer_ID,
		case when count(*) = 1 then 0 else
		datediff(max(Order_date), min(Order_date)) end as active_day
from order_table
GROUP BY Customer_ID;

-- I. Which regions have the highest concentration of high-value customers (by sales)?

select region, count(Customer_ID) as concentration
FROM(
		select customer_table.Customer_ID, customer_table.region, sum(order_table.sales) as total_sales
		from order_table
		join customer_table on order_table.Customer_ID = customer_table.Customer_ID
		GROUP BY customer_table.Customer_ID, customer_table.region) as t3
where total_sales > 1000
GROUP BY region
order by concentration desc;

-- 3. PRODUCT PERFORMANCE --------------------------
-- J. Which products consistently generate the highest sales over time?

select Product_ID, count(*) as top_5
from (
		SELECT  Product_ID, 
				sum(sales) as total_sales,
				rank() over(partition by Year(order_date), month(order_date) order by sum(sales) desc) as ranks
		from order_table
		GROUP BY Product_ID, Year(order_date), month(order_date)) as t4
where ranks<=5
GROUP BY Product_ID;

-- K. Which categories drive most of the order volume?

select pt.category, sum(ot.sales) as volume
from order_table as ot
join product_table as pt
on ot.Product_ID = pt.Product_ID
GROUP BY pt.Category
order by volume desc;

-- L. Are there products with declining sales trends?

select  product_id, 
		sales, 
        lag(sales) over (PARTITION BY product_id order by order_date) as previous_sales,
        sales - lag(sales) over (PARTITION BY product_id order by order_date) as Diff
from order_table;

-- M. Which sub-categories show the fastest growth in sales?

WITH growth as (
		select  pt.Sub_category,
				year(ot.order_date) as orderYear,
				sum(ot.sales) as sale,
				LAG(sum(ot.sales)) 
						over(
							PARTITION BY pt.Sub_category 
							ORDER BY Year(ot.order_date)) 
						as previous_sale        
		from order_table ot
		JOIN product_table pt 
		ON pt.Product_ID = ot.Product_ID
		GROUP BY pt.Sub_category, year(ot.order_date))
SELECT orderYear, Sub_category,
		round(((sale - previous_sale)*100.0)/sale,2) as percentage_change
FROM growth;