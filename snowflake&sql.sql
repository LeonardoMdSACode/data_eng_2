create or replace temporary stage g2_stage
fire_format= (type = json)
credentials=(aws_key_id='<>'
aws_secret_key'<>')
url='s3://datalake/erp/customer'        -- example url, use your own cloud storage provider

-- idea:
-- select * from @g2_stage
-- select * from vendor_rating
-- select * from vendor_competitor_rating
-- select * from vendor_competitor_comparison

select * from @g2_stage


create table as raw.rating_stars 
as
select lower(g2_flat.value ['input'] ['company_name']) AS company_name
     ,cast(value ['number_of_reviews'] AS INT) AS number_of_reviews
     ,cast(value ['number_of_stars'] AS NUMERIC(38, 2)) AS star_rating     
     ,value ['categories_on_g2'] category_list
from (
          select $1 json_data 
          from @g2_stage src
          ) g2
          ,lateral flatten(input => g2.json_data) g2_flat
union all

select lower(g2_flat_competitors.value ['competitor_name']) as company_name
     ,cast(g2_flat_competitors.value ['number_of_reviews'] AS INT) AS number_of_reviews
     ,cast(g2_flat_competitors.value ['number_of_stars'] AS NUMERIC(38, 2)) AS star_rating
     ,g2_flat_competitors.value ['product_category'] as categories_list
from (
     select $1 json_data
     from @stage_test src
     ) g2
     ,lateral flatten(input => g2.json_data) g2_flat
     ,lateral flatten(input => g2_flat.value ['top_10_competitors']) g2_flat_competitors --nested value




create table as raw.competitor_rating_stars 
as
     select
      lower(g2_flat.value ['input'] ['company_name']) AS company_name
     ,lower(g2_flat_competitors.value ['competitor_name']) as competitor_name
     ,cast(g2_flat_competitors.value ['number_of_reviews'] AS INT) AS number_of_reviews
     ,cast(g2_flat_competitors.value ['number_of_stars'] AS NUMERIC(38, 2)) AS star_rating
from (
     select $1 json_data
     from @stage_test src
     ) g2
     ,lateral flatten(input => g2.json_data) g2_flat
     ,lateral flatten(input => g2_flat.value ['top_10_competitors']) g2_flat_competitors




-- category
create
     or replace table raw.rating_stars_category AS
select
     ,lower(g2_flat_competitors.value ['competitor_name']) competitor_name
     ,competitor_category.value AS company_category
from (
     select $1 json_data
     from @stage_test src
     ) g2
     ,lateral flatten(input => g2.json_data) g2_flat
     ,lateral flatten(input => g2_flat.value ['top_10_competitors']) g2_flat_competitors
     ,lateral flatten(input => g2_flat.competitor.value ['product_category']) competitor_category

UNION all

SELECT lower(g2_flat.value ['input'] ['company_name'])
     ,category_flat.value
FROM (
    SELECT $1 json_data
    FROM @g2_stage src
    ) g2
    ,lateral flatten(input => g2.json_data) g2_flat
    ,lateral flatten(input => g2_flat.value ['categories_on_g2']) category_flat;




create or replace table raw.rating_stars_values AS
select sum(v.number_of_reviews * v.star_rating) * 1.0 / sum(number_of_reviews) avg_stars_category
     ,competitor_category
from raw.rating_stars v
join raw.rating_stars_category c
on v.company_name = c.company_name
group by company_category





CREATE TABLE rating_stars_category_comparison AS

select avg_stars_category
     ,vc.company_name
     ,categories_list
     ,cr.company_category
     ,star_rating
from vendor_category_rating cr
join vendor_category vc on vc.company_category = cr.company_category
join vendor_rating vr ON replace(vc.company_name, '"', '') = replace(vr.company_name, '"', '')

