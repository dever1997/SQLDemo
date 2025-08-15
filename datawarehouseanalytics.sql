
-- detect duplicate
select customer_key, count(*) 
from gold.dim_customers
group by customer_key
having count(*) > 1;

select order_number, count(*) as quantity
from gold.fact_sales
group by order_number
having count(*) > 1
order by quantity desc;

select first_name, last_name, country, category, sum(sales_amount) as sales_per_customer_per_products
from gold.fact_sales s
join gold.dim_customers c
on s.customer_key = c.customer_key
join gold.dim_products p
on s.product_key = p.product_key
group by first_name, last_name, country, category
having sum(sales_amount) > 10000
order by sales_per_customer_per_products desc
limit 100;

-- quantity sold by category
select p.category,
       count(*) as quantity_sold
from gold.fact_sales f
join gold.dim_products p
on f.product_key = p.product_key
group by p.category;


-- change-over-time
select  extract(year from order_date) as sales_per_year, 
		extract(month from order_date) as sales_per_month,
		sum(sales_amount) sales_amount
from  gold.fact_sales
where order_date is not null
group by rollup(extract(year from order_date), extract(month from order_date))
order by  extract(year from order_date), extract(month from order_date);

select  extract(year from order_date) as years, 
		sum(sales_amount) as sales_per_year
from  gold.fact_sales
where order_date is not null
group by rollup(extract(year from order_date))
order by extract(year from order_date) nulls last;

-- cumulative analysis
select year,
       month,
       sales_amount,
       sum(sales_amount) over (order by year, month rows between unbounded preceding and current row) as running_total
from(
		select extract(year from order_date) as year, 
		       extract(month from order_date) as month,
		       sum(sales_amount) as sales_amount  
		from gold.fact_sales
		where order_date is not null
		group by extract(year from order_date), extract(month from order_date)
);

select year,
       yearly_sales,
	   sum(yearly_sales) over(order by year) as running_total,
	   cume_dist() over(order by year) as percents
from(
		select extract(year from order_date) as year, 
			   sum(sales_amount) as yearly_sales  
		from gold.fact_sales
		where order_date is not null
		group by extract(year from order_date)
);

with sales_date as (
       select  extract(year from order_date) as year, 
		       extract(month from order_date) as month,
		       sum(sales_amount) as sales_amount  
		from gold.fact_sales
		where order_date is not null
		group by extract(year from order_date), extract(month from order_date)
)
select year,
	   month,
	   sales_amount,
	   sum(sales_amount) over (order by year, month) as running_total
	   from sales_date;

select year,
	   avg_sales_per_year,
	   round(avg(avg_sales_per_year) over(order by year rows between 2 preceding and current row), 2) as two_year_moving_average
from(
		select extract(year from order_date ) as year,
		       round(avg(sales_amount), 2) as avg_sales_per_year
		from gold.fact_sales
		where order_date is not null
		group by extract(year from order_date));


-- performance analysis

select year,
       yearly_sales,
	   lead(yearly_sales,1,0) over(order by year) as next_year_sales,
	   concat(round( ( lead(yearly_sales,1) over(order by year) - cast(yearly_sales as numeric) ) / yearly_sales, 3),' %') as year_over_year_change
from(
		select extract(year from order_date) as year, 	      
		       sum(sales_amount) as yearly_sales
		from gold.fact_sales
		where order_date is not null
		group by extract(year from order_date));

select year,
       yearly_sales,
	   first_value(yearly_sales) over(order by year) as first_year_sales,
	   concat(round( (cast(yearly_sales as numeric) - first_value(yearly_sales) over(order by year) ) / yearly_sales, 2), ' %') as changes_since_first_year
from(
		select extract(year from order_date) as year, 	      
		       sum(sales_amount) as yearly_sales
		from gold.fact_sales
		where order_date is not null
		group by extract(year from order_date)
		offset 1);

-- part-to-whole analysis

total_sales_per_year as (
    select extract(year from order_date) as years,
		   sum(sales_amount) as yearly_sales
	from gold.fact_sales
	where order_date is not null
	group by extract(year from order_date)
)
select years, 
       yearly_sales, 
	   sum(yearly_sales) over() as total_sales,
	   concat(round((yearly_sales / sum(yearly_sales) over()) * 100, 2),' %') as percentage_sales
       from total_sales_per_year;

select category,
       sales_per_category,
	   sum(sales_per_category) over () as total_sales,
	   concat(round((sales_per_category / sum(sales_per_category) over()) * 100, 2),' %') as percentage_sales
from 
	(select category,
	        sum(sales_amount) as sales_per_category
	from gold.fact_sales s
	inner join gold.dim_products p
	on s.product_key = p.product_key
	where order_date is not null
	group by category
);

-- Data segmentation

select first_name,
       last_name,
	   sales,
	   case 
		   when sales >= 1000 then 'High Value'
		   when sales >= 500 then 'Normal'
		   else 'Low value' 
	   end as spending_behavior
from(
	select c.first_name,
	       c.last_name,
	       sum(sales_amount) as sales
	from gold.fact_sales f
	inner join gold.dim_customers c
	on f.customer_key = c.customer_key
	where order_date is not null
	group by c.first_name, c.last_name
)
order by sales DESC;


select product_name,
       quantity,
	   case
		   when quantity >= 1000 then 'best-selling'
		   when quantity >= 500 then 'good products'
		   else 'low volume'
	   end as status
from(
		select p.product_name,
		       count(f.product_key) as quantity
		from gold.fact_sales f
		inner join gold.dim_products p
		on f.product_key = p.product_key
		group by p.product_name
)
order by quantity DESC;

