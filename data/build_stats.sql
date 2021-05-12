insert into batch_log values(null, 1, 'build stats', now(), 'apt_sale_stats N');

truncate apt_sale_stats;
insert into apt_sale_stats
        select region_key, 3, made_year, area_type, ym, 'N', avg(price/(area/3.3)), count(*)
          from apt_sale_new a, apt_master b
         where a.apt_id = b.id
         group by region_key, made_year, area_type, ym;

insert into batch_log values(null, 2, 'build stats', now(), 'apt_sale_stats Y');
insert into apt_sale_stats
        select region_key, 3, made_year, area_type, ym, 'Y', avg(price/(area/3.3)), count(*)
          from apt_sale_new a, apt_master b
         where a.apt_id = b.id and b.k_apt_id is not null
         group by region_key, made_year, area_type, ym;

insert into batch_log values(null, 3, 'build stats', now(), 'apt_sale_stats 2');
insert into apt_sale_stats
        select r.upper_region, 2, made_year, area_type, ym, danji_flag, (sum(unit_price * cnt) / sum(cnt)), sum(cnt)
          from apt_sale_stats a, region_info r
         where a.region_key = r.region_key
           and a.level = 3
         group by r.upper_region, made_year, area_type, ym, danji_flag;

insert into batch_log values(null, 4, 'build stats', now(), 'apt_sale_stats 1');
insert into apt_sale_stats
        select r.upper_region, 1, made_year, area_type, ym, danji_flag, (sum(unit_price * cnt) / sum(cnt)), sum(cnt)
          from apt_sale_stats a, region_info r
         where a.region_key = r.region_key
           and a.level = 2
         group by r.upper_region, made_year, area_type, ym, danji_flag;

insert into batch_log values(null, 5, 'build stats', now(), 'apt_sale_stats 0');
insert into apt_sale_stats
        select '0000000000', 0, made_year, area_type, ym, danji_flag, (sum(unit_price * cnt) / sum(cnt)), sum(cnt)
          from apt_sale_stats a
         where a.level = 1
         group by made_year, area_type, ym, danji_flag;


insert into batch_log values(null, 6, 'build stats', now(), 'apt_region_ma ~ 201012');
truncate apt_region_ma;
insert into apt_region_ma
        select *
          from (
            select a.region_key, a.level, a.danji_flag, b.ym, a.made_year, a.area_type, round(sum(a.unit_price*a.cnt)/sum(a.cnt), 2) unit_price, sum(a.cnt) cnt
              from tmp_ym b, apt_sale_stats a
             where b.ym between '200601' and '201012'
			   and a.ym between date_format(date_sub(str_to_date(concat(b.ym,'01'), '%Y%m%d'), interval 11 month), '%Y%m') and b.ym
             group by b.ym, a.region_key, a.level, a.danji_flag, a.made_year, a.area_type
          ) a;

insert into batch_log values(null, 7, 'build stats', now(), 'apt_region_ma ~ 201512');
insert into apt_region_ma
        select *
          from (
            select a.region_key, a.level, a.danji_flag, b.ym, a.made_year, a.area_type, round(sum(a.unit_price*a.cnt)/sum(a.cnt), 2) unit_price, sum(a.cnt) cnt
              from tmp_ym b, apt_sale_stats a
             where b.ym between '201101' and '201512'
			   and a.ym between date_format(date_sub(str_to_date(concat(b.ym,'01'), '%Y%m%d'), interval 11 month), '%Y%m') and b.ym
             group by b.ym, a.region_key, a.level, a.danji_flag, a.made_year, a.area_type
          ) a;

insert into batch_log values(null, 8, 'build stats', now(), 'apt_region_ma ~ 202012');
insert into apt_region_ma
        select *
          from (
            select a.region_key, a.level, a.danji_flag, b.ym, a.made_year, a.area_type, round(sum(a.unit_price*a.cnt)/sum(a.cnt), 2) unit_price, sum(a.cnt) cnt
              from tmp_ym b, apt_sale_stats a
             where b.ym between '201601' and '202012'
			   and a.ym between date_format(date_sub(str_to_date(concat(b.ym,'01'), '%Y%m%d'), interval 11 month), '%Y%m') and b.ym
             group by b.ym, a.region_key, a.level, a.danji_flag, a.made_year, a.area_type
          ) a;

insert into batch_log values(null, 9, 'build stats', now(), 'apt_region_ma ~ 202103');
insert into apt_region_ma
        select *
          from (
            select a.region_key, a.level, a.danji_flag, b.ym, a.made_year, a.area_type, round(sum(a.unit_price*a.cnt)/sum(a.cnt), 2) unit_price, sum(a.cnt) cnt
              from tmp_ym b, apt_sale_stats a
             where b.ym between '202101' and '202103'
			   and a.ym between date_format(date_sub(str_to_date(concat(b.ym,'01'), '%Y%m%d'), interval 11 month), '%Y%m') and b.ym
             group by b.ym, a.region_key, a.level, a.danji_flag, a.made_year, a.area_type
          ) a;

insert into batch_log values(null, 10, 'build stats', now(), 'apt_qbox_stats 1 3');
truncate apt_qbox_stats;
SET GROUP_CONCAT_MAX_LEN = 10485760;
insert into apt_qbox_stats
        select region_key, level, '1', danji, ym
             , cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
             , cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)
             , 1q_price, 3q_price, med_price, avg_price
          from (
            select a.ym, r.region_key, r.level, ifnull(danji_flag, 'N') danji
                 , max(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) max_price_id
                 , min(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) min_price_id
                 , substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price
                 , substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price
                 , substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price
                 , round(avg(price), 2) avg_price
	from region_info r, apt_sale_new a, apt_master m
	where r.level = 3
           and r.region_key = m.region_key
           and m.id = a.apt_id
	 group by a.ym, r.region_key, r.level, ifnull(danji_flag, 'N')) a;

insert into batch_log values(null, 11, 'build stats', now(), 'apt_qbox_stats 1 2');
insert into apt_qbox_stats
select region_key, level, '1', danji, ym
             , cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
             , cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)
             , 1q_price, 3q_price, med_price, avg_price
          from (
            select a.ym, r.region_key, r.level, ifnull(danji_flag, 'N') danji
                 , max(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) max_price_id
                 , min(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) min_price_id
                 , substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price
                 , substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price
                 , substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price
                 , round(avg(price), 2) avg_price
	from region_info r, region_info r1, apt_sale_new a, apt_master m
	where r.level = 2
           and r.region_key = r1.upper_region
           and r1.region_key = m.region_key
           and m.id = a.apt_id
	 group by a.ym, r.region_key, r.level, ifnull(danji_flag, 'N')) a;


insert into batch_log values(null, 12, 'build stats', now(), 'apt_qbox_stats 1 1');
insert into apt_qbox_stats
select region_key, level, '1', danji, ym
             , cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
             , cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)
             , 1q_price, 3q_price, med_price, avg_price
          from (
            select a.ym, r.region_key, r.level, ifnull(danji_flag, 'N') danji
                 , max(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) max_price_id
                 , min(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) min_price_id
                 , substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price
                 , substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price
                 , substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price
                 , round(avg(price), 2) avg_price
	from region_info r, region_info r1, region_info r2, apt_sale_new a, apt_master m
	where r.level = 1
           and r.region_key = r1.upper_region
           and r1.region_key = r2.upper_region
           and r2.region_key = m.region_key
           and m.id = a.apt_id
	 group by a.ym, r.region_key, r.level, ifnull(danji_flag, 'N')) a;


insert into batch_log values(null, 13, 'build stats', now(), 'apt_qbox_stats 1 0');
insert into apt_qbox_stats
select region_key, level, '1', danji, ym
             , cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
             , cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)
             , 1q_price, 3q_price, med_price, avg_price
          from (
            select a.ym, r.region_key, r.level, ifnull(danji_flag, 'N') danji
                 , max(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) max_price_id
                 , min(concat(lpad(price/(area/3.3),12,'0'),lpad(a.id,12,'0'))) min_price_id
                 , substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price
                 , substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price
                 , substring_index(substring_index(group_concat(price/(area/3.3) order by price/(area/3.3) asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price
                 , round(avg(price), 2) avg_price
	from (select '0000000000' region_key, 0 level from dual) r, apt_sale_new a, apt_master m
	 where m.id = a.apt_id
	 group by a.ym, r.region_key, r.level, ifnull(danji_flag, 'N')) a;


insert into batch_log values(null, 14, 'build stats', now(), 'apt_qbox_stats 2 3');
insert into apt_qbox_stats
        select region_key, level, '2', danji, ym
             , cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
             , cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)
             , 1q_price, 3q_price, med_price, avg_price
          from (
            select a.ym, r.region_key, r.level, ifnull(danji_flag, 'N') danji
                 , max(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) max_price_id
                 , min(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) min_price_id
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price
                 , round(avg(price), 2) avg_price
	from region_info r, apt_sale_new a, apt_master m
	where r.level = 3
           and r.region_key = m.region_key
           and m.id = a.apt_id
	 group by a.ym, r.region_key, r.level, ifnull(danji_flag, 'N')) a;


insert into batch_log values(null, 15, 'build stats', now(), 'apt_qbox_stats 2 2');
insert into apt_qbox_stats
        select region_key, level, '2', danji, ym
             , cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
             , cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)
             , 1q_price, 3q_price, med_price, avg_price
          from (
            select a.ym, r.region_key, r.level, ifnull(danji_flag, 'N') danji
                 , max(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) max_price_id
                 , min(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) min_price_id
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price
                 , round(avg(price), 2) avg_price
	from region_info r, region_info r1, apt_sale_new a, apt_master m
	where r.level = 2
           and r.region_key = r1.upper_region
           and r1.region_key = m.region_key
           and m.id = a.apt_id
	 group by a.ym, r.region_key, r.level, ifnull(danji_flag, 'N')) a;


insert into batch_log values(null, 16, 'build stats', now(), 'apt_qbox_stats 2 1');
insert into apt_qbox_stats
        select region_key, level, '2', danji, ym
             , cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
             , cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)
             , 1q_price, 3q_price, med_price, avg_price
          from (
            select a.ym, r.region_key, r.level, ifnull(danji_flag, 'N') danji
                 , max(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) max_price_id
                 , min(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) min_price_id
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price
                 , round(avg(price), 2) avg_price
	from region_info r, region_info r1, region_info r2, apt_sale_new a, apt_master m
	where r.level = 1
           and r.region_key = r1.upper_region
           and r1.region_key = r2.upper_region
           and r2.region_key = m.region_key
           and m.id = a.apt_id
	 group by a.ym, r.region_key, r.level, ifnull(danji_flag, 'N')) a;


insert into batch_log values(null, 17, 'build stats', now(), 'apt_qbox_stats 2 0');
insert into apt_qbox_stats
        select region_key, level, '2', danji, ym
             , cast(substr(max_price_id, 1, 12) as double), cast(substr(max_price_id, 13, 12) as signed integer)
             , cast(substr(min_price_id, 1, 12) as double), cast(substr(min_price_id, 13, 12) as signed integer)
             , 1q_price, 3q_price, med_price, avg_price
          from (
            select a.ym, r.region_key, r.level, ifnull(danji_flag, 'N') danji
                 , max(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) max_price_id
                 , min(concat(lpad(price,12,'0'),lpad(a.id,12,'0'))) min_price_id
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((50 / 100) * count(0)) + 1)),',',-1) as med_price
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((25 / 100) * count(0)) + 1)),',',-1) as 1q_price
                 , substring_index(substring_index(group_concat(price order by price asc separator ','), ',', (((75 / 100) * count(0)) + 1)),',',-1) as 3q_price
                 , round(avg(price), 2) avg_price
	from (select '0000000000' region_key, 0 level from dual) r, apt_sale_new a, apt_master m
	 where m.id = a.apt_id
	 group by a.ym, r.region_key, r.level, ifnull(danji_flag, 'N')) a;
