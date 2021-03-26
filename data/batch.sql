SET GROUP_CONCAT_MAX_LEN = 10485760;
insert into batch_log values(null, 1, 'insert qbox_stats before 201501', now(), 'starting gubun 1, level 3');
insert into apt_qbox_stats      
select region_cd, upper_cd, level, '1', danji, ym
, cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
, cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)     
, 1q_price, 3q_price, med_price, avg_price     
from (                 
  select a.ym, r.region_cd, r.upper_cd, r.level, ifnull(danji_flag, 'N') danji      
, max(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) max_price_id      
, min(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) min_price_id                 
, substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price       
, substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price       
, substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price      
, round(avg(price), 2) avg_price  
from region_cd r, apt_sale_new a, apt_master m    
where r.level = 3       and valid = 'Y'       and r.region_cd = m.region_cd      and r.upper_cd = m.upper_cd      and m.id = a.apt_id      and a.ym < '201501' 
group by a.ym, r.upper_cd, r.region_cd, r.level, ifnull(danji_flag, 'N')
) a;

insert into batch_log values(null, 2, 'insert qbox_stats', now(), 'starting gubun 1, level 2');
insert into apt_qbox_stats      
select region_cd, upper_cd, level, '1', danji, ym
, cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
, cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)     
, 1q_price, 3q_price, med_price, avg_price     
from (                 
  select a.ym, r.region_cd, r.upper_cd, r.level, ifnull(danji_flag, 'N') danji      
, max(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) max_price_id      
, min(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) min_price_id                 
, substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price       
, substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price       
, substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price      
, round(avg(price), 2) avg_price  
from region_cd r, apt_sale_new a, apt_master m    
where r.level = 2
           and valid = 'Y'
           and r.region_cd = m.upper_cd
           and m.id = a.apt_id
           and a.ym < '201501' 
group by a.ym, r.upper_cd, r.region_cd, r.level, ifnull(danji_flag, 'N')
) a;

insert into batch_log values(null, 3, 'insert qbox_stats', now(), 'starting gubun 1, level 1');
insert into apt_qbox_stats      
select region_cd, upper_cd, level, '1', danji, ym
, cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
, cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)     
, 1q_price, 3q_price, med_price, avg_price     
from (                 
  select a.ym, r.region_cd, r.upper_cd, r.level, ifnull(danji_flag, 'N') danji      
, max(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) max_price_id      
, min(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) min_price_id                 
, substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price       
, substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price       
, substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price      
, round(avg(price), 2) avg_price  
from region_cd r, region_cd r1, apt_sale_new a, apt_master m
where r.level = 1
           and r.valid = 'Y'
           and r.region_cd = r1.upper_cd
           and r1.valid = 'Y'
           and r1.level = 2
           and r1.region_cd = m.upper_cd
           and m.id = a.apt_id
           and a.ym < '201501' 
group by a.ym, r.upper_cd, r.region_cd, r.level, ifnull(danji_flag, 'N')
) a;

insert into batch_log values(null, 4, 'insert qbox_stats', now(), 'starting gubun 1, level 0');
insert into apt_qbox_stats      
select region_cd, upper_cd, level, '1', danji, ym
, cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
, cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)     
, 1q_price, 3q_price, med_price, avg_price     
from (                 
  select a.ym, r.region_cd, r.upper_cd, r.level, ifnull(danji_flag, 'N') danji      
, max(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) max_price_id      
, min(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) min_price_id                 
, substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price       
, substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price       
, substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price      
, round(avg(price), 2) avg_price  
from (select '00000' region_cd, '00000' upper_cd, 0 level from dual) r, apt_sale_new a, apt_master m   
where m.id = a.apt_id
           and a.ym < '201501' 
group by a.ym, r.upper_cd, r.region_cd, r.level, ifnull(danji_flag, 'N')
) a;

insert into batch_log values(null, 5, 'insert qbox_stats', now(), 'starting gubun 2, level 3');
insert into apt_qbox_stats
        select region_cd, upper_cd, level, '2', danji, ym
             , cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
             , cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)
             , 1q_price, 3q_price, med_price, avg_price
          from (
            select a.ym, r.region_cd, r.upper_cd, r.level, ifnull(danji_flag, 'N') danji
                 , max(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) max_price_id
                 , min(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) min_price_id
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price
                 , round(avg(price), 2) avg_price
from region_cd r, apt_sale_new a, apt_master m    
where r.level = 3       and valid = 'Y'       and r.region_cd = m.region_cd      and r.upper_cd = m.upper_cd      and m.id = a.apt_id      and a.ym < '201501' 
group by a.ym, r.upper_cd, r.region_cd, r.level, ifnull(danji_flag, 'N')
) a;

insert into batch_log values(null, 6, 'insert qbox_stats', now(), 'starting gubun 2, level 2');
insert into apt_qbox_stats
        select region_cd, upper_cd, level, '2', danji, ym
             , cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
             , cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)
             , 1q_price, 3q_price, med_price, avg_price
          from (
            select a.ym, r.region_cd, r.upper_cd, r.level, ifnull(danji_flag, 'N') danji
                 , max(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) max_price_id
                 , min(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) min_price_id
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price
                 , round(avg(price), 2) avg_price
from region_cd r, apt_sale_new a, apt_master m    
where r.level = 2
           and valid = 'Y'
           and r.region_cd = m.upper_cd
           and m.id = a.apt_id
           and a.ym < '201501' 
group by a.ym, r.upper_cd, r.region_cd, r.level, ifnull(danji_flag, 'N')
) a;

insert into batch_log values(null, 7, 'insert qbox_stats', now(), 'starting gubun 2, level 1');
insert into apt_qbox_stats
        select region_cd, upper_cd, level, '2', danji, ym
             , cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
             , cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)
             , 1q_price, 3q_price, med_price, avg_price
          from (
            select a.ym, r.region_cd, r.upper_cd, r.level, ifnull(danji_flag, 'N') danji
                 , max(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) max_price_id
                 , min(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) min_price_id
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price
                 , round(avg(price), 2) avg_price
from region_cd r, region_cd r1, apt_sale_new a, apt_master m
where r.level = 1
           and r.valid = 'Y'
           and r.region_cd = r1.upper_cd
           and r1.valid = 'Y'
           and r1.level = 2
           and r1.region_cd = m.upper_cd
           and m.id = a.apt_id
           and a.ym < '201501' 
group by a.ym, r.upper_cd, r.region_cd, r.level, ifnull(danji_flag, 'N')
) a;

insert into batch_log values(null, 8, 'insert qbox_stats', now(), 'starting gubun 2, level 0');
insert into apt_qbox_stats
        select region_cd, upper_cd, level, '2', danji, ym
             , cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
             , cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)
             , 1q_price, 3q_price, med_price, avg_price
          from (
            select a.ym, r.region_cd, r.upper_cd, r.level, ifnull(danji_flag, 'N') danji
                 , max(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) max_price_id
                 , min(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) min_price_id
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price
                 , round(avg(price), 2) avg_price
from (select '00000' region_cd, '00000' upper_cd, 0 level from dual) r, apt_sale_new a, apt_master m   
where m.id = a.apt_id
           and a.ym < '201501' 
group by a.ym, r.upper_cd, r.region_cd, r.level, ifnull(danji_flag, 'N')
) a;

insert into batch_log values(null, 9, 'insert qbox_stats', now(), completed gubun 2, level 0');
