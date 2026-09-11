INSERT INTO {{ sink('cosmos', 'orders_out') }}
select id, region, n, name, tags, "address.city" as city
from {{ source('cosmos', 'orders_in') }}
order by n
