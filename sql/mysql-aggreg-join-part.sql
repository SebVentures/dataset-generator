create table aggreg_join_full as 
select date, acquisition_channel, persona, count(*) as nb, sum(mrr) as mrr
from lifetime 
inner join customer on customer_id = id
where year(date) = 2019
group by 1, 2, 3