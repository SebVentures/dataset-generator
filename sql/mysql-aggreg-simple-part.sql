create table aggreg_simple_part as 
select date, sum(mrr) as mrr
from lifetime
where year(date) = 2019
group by date
order by 1