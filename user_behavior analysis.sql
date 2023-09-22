-- CREATE TABLE
Create table if not exists user_behavior_2 as
select * from user_behavior limit 5000000;

-- CHECK NULL VALUE
SELECT count(user_id),count(item_id),count(category_id),count(behavior_type),count(timestamp)
from user_behavior_2;

-- CHECK DUPLICATE values
select user_id,item_id,timestamp,count(*) as count from user_behavior_2
group by user_id,item_id,timestamp
having count(*)>1;

-- Remove duplicates
alter table user_behavior_2 add id int primary key auto_increment first;

DELETE user_behavior_2
FROM user_behavior_2
JOIN (
    SELECT user_id, item_id, timestamp, MIN(id) AS id
    FROM user_behavior_2
    GROUP BY user_id, item_id, timestamp
    HAVING COUNT(*) > 1
    ) AS t2
ON user_behavior_2.user_id = t2.user_id
AND user_behavior_2.item_id = t2.item_id
AND user_behavior_2.timestamp = t2.timestamp
AND user_behavior_2.id > t2.id;

-- Add Date Time Hour
-- Datetime
show VARIABLES like '%_buffer%';
set GLOBAL innodb_buffer_pool_size=1070000000;
alter table user_behavior_2 add datetimes TIMESTAMP(0);
update user_behavior_2 set datetimes=FROM_UNIXTIME(timestamp);
select * from user_behavior_2;
-- Date, Time, Hour
alter table user_behavior_2 add date char(10),
							add time char(8), 
                            add hour char(2);
update user_behavior_2 set date=Date(datetimes), 
						   time=time(datetimes), 
                           hour=Hour(datetimes);

-- Check and Remove Data Anomalies
select max(datetimes),min(datetimes) from user_behavior_2;
delete from user_behavior_2
where date < '2017-11-25'
or date > '2017-12-03';

select count(*) as Number_of_Records from user_behavior_2;

-- pv, uv, average traffic rate: pv/uv
Drop table if exists pv_uv_puv;
Create table pv_uv_puv as
select date,
	count(*) as pv,
	count(distinct user_id) as uv,
    round(count(*)/count(distinct user_id),1) as 'pv/uv'
from user_behavior_2
where behavior_type='pv'
GROUP BY date;

-- Retention
-- day 1 retention rate, 3-day retention rate, 7-day retention rate
Drop Table if Exists RetentionRate;
Create Table RetentionRate as
select a.date,
	count(if(datediff(b.date,a.date)=1,b.user_id,null))/count(if(datediff(b.date,a.date)=0,b.user_id,null)) as Day_1_RentionRate,
	count(if(datediff(b.date,a.date)=3,b.user_id,null))/count(if(datediff(b.date,a.date)=0,b.user_id,null)) as Day_3_RentionRate,
	count(if(datediff(b.date,a.date)=7,b.user_id,null))/count(if(datediff(b.date,a.date)=0,b.user_id,null)) as Day_7_RentionRate
from
(select user_id,date
from user_behavior_2
group by user_id,date) a
join
(select user_id,date
from user_behavior_2
group by user_id,date) b
where a.user_id=b.user_id and a.date<=b.date
group by a.date;

-- Bounce number
select count(*) as Bounce_Users
from
(select user_id from user_behavior_2
group by user_id
having count(behavior_type)=1) a;

-- Bounce Rate--0
SELECT SUM(pv) AS total_pv, 
       (
           SELECT COUNT(*) as bounce_users
           FROM (
               SELECT user_id
               FROM user_behavior_2
               GROUP BY user_id
               HAVING COUNT(behavior_type) = 1
           ) a
       ) / SUM(pv) AS bounce_rate
FROM pv_uv_puv;

-- Time dimension 
Drop table if exists date_hour_behavior;
Create table date_hour_behavior as 
select date,hour,
	count(if(behavior_type='pv',behavior_type,null)) as 'pv',
    count(if(behavior_type='cart',behavior_type,null)) as 'cart',
    count(if(behavior_type='fav',behavior_type,null)) as 'fav',
    count(if(behavior_type='buy',behavior_type,null)) as 'buy'
from user_behavior_2
group by date,hour
order by date,hour;

-- Behavior dimension
-- User Conversion Rate
Drop table if exists behavior_user_num;
Create table behavior_user_num as
select behavior_type,
	count(DISTINCT user_id) as user_num
from user_behavior_2
group by behavior_type
order by behavior_type desc;
-- buy/pv
select (select user_num
from behavior_user_num
where behavior_type='buy')/(select user_num from behavior_user_num where behavior_type='pv') as ratio;-- 66%

-- calculate the Number of each behavior types (funnel mdoel)
Drop table if exists behavior_num;
Create table behavior_num as
select behavior_type,
count(*) as user_num
from user_behavior_2
group by behavior_type
order by behavior_type desc;

-- Calculate the purchase rate of pv behavior.
select (select user_num
from behavior_num
where behavior_type='buy')/(select user_num from behavior_num where behavior_type='pv') as ratio;-- 2.23%

-- Calcualte the pv rate of cart and fav
select ((select user_num
from behavior_num
where behavior_type='fav')+(select user_num
from behavior_num
where behavior_type='cart'))/(select user_num from behavior_num where behavior_type='pv') as ratio;-- 9.5%

-- User Behaviors on Each Product
create view user_behavior_view as
select user_id,item_id,
	count(if(behavior_type='pv',behavior_type,null)) as 'pv',
	count(if(behavior_type='fav',behavior_type,null)) as 'fav',
    count(if(behavior_type='cart',behavior_type,null))as 'cart',
    count(if(behavior_type='buy',behavior_type,null)) as 'buy'
from user_behavior_2
group by user_id,item_id;

-- Standardizded User Behavior
create view user_behavior_standard as
select user_id,
	item_id,
    (case when pv>0 then 1 else 0 end) viewed,
    (case when fav>0 then 1 else 0 end) favorited,
    (case when cart>0 then 1 else 0 end) cart,
    (case when buy>0 then 1 else 0 end) bought
from user_behavior_view;

select sum(buy)
from user_behavior_view
where buy>0 and fav=0 and cart=0; -- 71541

Select 94924-71541; -- 23383 the purchase number after add cart and fav
Select 23383/((select user_num
from behavior_num
where behavior_type='fav')+(select user_num
from behavior_num
where behavior_type='cart'));-- 5.79%

-- RFM
-- Last Order dates
Drop table if exists RFM_Model;
Create table RFM_Model as
select user_id, 
	max(date) as "Last_Order_Date",
    datediff("2017-12-03",max(date)) as "R",
    count(user_id) as "F"
from user_behavior_2
where behavior_type="buy"
group by user_id
order by "R" desc, "F" desc;


alter table RFM_Model add column R_Score int, add F_Score int;
Update RFM_Model
set R_Score=
(case when R between 0 and 1 then 5
	  when R between 2 and 3 then 4
      when R between 4 and 5 then 3
      when R between 6 and 7 then 2
      else 1
end);
Update RFM_Model
set F_Score=
(case when F between 1 and 4 then 1
	  when F between 5 and 9 then 2
      when F between 10 and 14 then 3
      when F between 15 and 19 then 4
      else 5
end);


set @F_avg=null;
set @R_avg=null;
select avg(F_score) into @F_avg from Rfm_model;
select avg(R_score) into @R_avg from Rfm_model;
alter table Rfm_model add column Customer_Segmentation varchar(40);
update Rfm_model
set Customer_Segmentation = case
when f_score>@f_avg and r_score>@r_avg then "Champion"
when f_score<@f_avg and r_score>@r_avg then "Potential Loyalist"
when f_score>@f_avg and r_score<@r_avg then "Loyal Customer"
when f_score<@f_avg and r_score<@r_avg then "At-Risk Customer"
end;

Drop table if exists TotalNumber_Customer_Segmentation;
Create Table TotalNumber_Customer_Segmentation as
select Customer_segmentation,count(user_id) as Total_Number
from rfm_model
group by customer_segmentation
order by Total_Number desc;

-- product demension 
-- popular categories 
create table popular_categories(
category_id int,
pv int);
insert into popular_categories
select category_id
,count(if(behavior_type='pv',behavior_type,null)) 'category pv'
from user_behavior_2
GROUP BY category_id
order by 2 desc
limit 10;

create table popular_categories_purchase(
category_id int,
pv int);
insert into popular_categories_purchase
select category_id
,count(if(behavior_type='buy',behavior_type,null)) 'category purchases'
from user_behavior_2
GROUP BY category_id
order by 2 desc
limit 10;

-- Popular items
create table popular_items(
item_id int,
pv int);
insert into popular_items
select item_id
,count(if(behavior_type='pv',behavior_type,null)) as 'item pv'
from user_behavior_2
GROUP BY item_id
order by 2 desc
limit 10;

create table popular_items_purchases(
item_id int,
purchases int);
insert into popular_items_purchases
select item_id
,count(if(behavior_type='buy',behavior_type,null)) as 'item purchases'
from user_behavior_2
GROUP BY item_id
order by 2 desc
limit 10;









