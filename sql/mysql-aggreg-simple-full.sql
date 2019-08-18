create table aggreg_simple_full_3 as 
select date, sum(mrr) as mrr
from lifetime
group by date
order by 1