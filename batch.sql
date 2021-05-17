set @job_title = 'build stats for danji_flag = Y';
set @seq = 1;
insert into batch_log values(null, @seq, @job_title, now(), 'apt_sale_stats 3');

delete from apt_sale_stats where danji_flag = 'Y' and level = 3;
insert into apt_sale_stats
        select region_key, 3, made_year, area_type, a.ym, 'Y', round(avg(price/(area/3.3)), 2) unit_price, round(avg(price), 2) price, count(*)
          from apt_sale_new a, apt_master b
         where a.apt_id = b.id and ifnull(b.danji_flag, 'N') = 'Y'
         group by region_key, made_year, area_type, a.ym;

set @seq = @seq + 1;
insert into batch_log values(null, @seq, @job_title, now(), 'apt_sale_stats 2');
delete from apt_sale_stats where danji_flag = 'Y' and level = 2;
insert into apt_sale_stats
        select r.upper_region, 2, made_year, area_type, a.ym, danji_flag, round((sum(unit_price * cnt) / sum(cnt)), 2), round((sum(price * cnt) / sum(cnt)), 2), sum(cnt)
          from apt_sale_stats a, region_info r
         where a.region_key = r.region_key
           and a.level = 3
		   and a.danji_flag = 'Y'
         group by r.upper_region, made_year, area_type, a.ym, danji_flag;

set @seq = @seq + 1;
insert into batch_log values(null, @seq, @job_title, now(), 'apt_sale_stats 1');
delete from apt_sale_stats where danji_flag = 'Y' and level = 1;
insert into apt_sale_stats
        select r.upper_region, 1, made_year, area_type, a.ym, danji_flag, round((sum(unit_price * cnt) / sum(cnt)), 2), round((sum(price * cnt) / sum(cnt)), 2), sum(cnt)
          from apt_sale_stats a, region_info r
         where a.region_key = r.region_key
           and a.level = 2
		   and a.danji_flag = 'Y'
         group by r.upper_region, made_year, area_type, a.ym, danji_flag;

set @seq = @seq + 1;
insert into batch_log values(null, @seq, @job_title, now(), 'apt_sale_stats 0');
delete from apt_sale_stats where danji_flag = 'Y' and level = 0;
insert into apt_sale_stats
        select '0000000000', 0, made_year, area_type, a.ym, danji_flag, round((sum(unit_price * cnt) / sum(cnt)), 2), round((sum(price * cnt) / sum(cnt)), 2), sum(cnt)
          from apt_sale_stats a
         where a.level = 1
		   and a.danji_flag = 'Y'
         group by made_year, area_type, a.ym, danji_flag;


set @seq = @seq + 1;
insert into batch_log values(null, @seq, @job_title, now(), 'apt_region_ma ~ 201012');
delete from apt_region_ma where danji_flag = 'Y';
insert into apt_region_ma
        select *
          from (
            select a.region_key, a.level, a.danji_flag, b.ym, a.made_year, a.area_type
				  , round(sum(a.unit_price*a.cnt)/sum(a.cnt), 2) unit_price, round(sum(a.price*a.cnt)/sum(a.cnt), 2) price, sum(a.cnt) cnt
              from tmp_ym b, apt_sale_stats a
             where b.ym between '200601' and '201012'
			   and a.ym between date_format(date_sub(str_to_date(concat(a.ym,'01'), '%Y%m%d'), interval 11 month), '%Y%m') and a.ym
			   and a.danji_flag = 'Y'
             group by b.ym, a.region_key, a.level, a.danji_flag, a.made_year, a.area_type
          ) a;

set @seq = @seq + 1;
insert into batch_log values(null, @seq, @job_title, now(), 'apt_region_ma ~ 201512');
insert into apt_region_ma
        select *
          from (
            select a.region_key, a.level, a.danji_flag, b.ym, a.made_year, a.area_type
				  , round(sum(a.unit_price*a.cnt)/sum(a.cnt), 2) unit_price, round(sum(a.price*a.cnt)/sum(a.cnt), 2) price, sum(a.cnt) cnt
              from tmp_ym b, apt_sale_stats a
             where b.ym between '201101' and '201512'
			   and a.ym between date_format(date_sub(str_to_date(concat(a.ym,'01'), '%Y%m%d'), interval 11 month), '%Y%m') and a.ym
			   and a.danji_flag = 'Y'
             group by b.ym, a.region_key, a.level, a.danji_flag, a.made_year, a.area_type
          ) a;

set @seq = @seq + 1;
insert into batch_log values(null, @seq, @job_title, now(), 'apt_region_ma ~ 202012');
insert into apt_region_ma
        select *
          from (
            select a.region_key, a.level, a.danji_flag, b.ym, a.made_year, a.area_type
				 , round(sum(a.unit_price*a.cnt)/sum(a.cnt), 2) unit_price, round(sum(a.price*a.cnt)/sum(a.cnt), 2) price, sum(a.cnt) cnt
              from tmp_ym b, apt_sale_stats a
             where b.ym between '201601' and '202012'
			   and a.ym between date_format(date_sub(str_to_date(concat(a.ym,'01'), '%Y%m%d'), interval 11 month), '%Y%m') and a.ym
			   and a.danji_flag = 'Y'
             group by b.ym, a.region_key, a.level, a.danji_flag, a.made_year, a.area_type
          ) a;

set @seq = @seq + 1;
insert into batch_log values(null, @seq, @job_title, now(), 'apt_region_ma ~ 202105');
insert into apt_region_ma
        select *
          from (
            select a.region_key, a.level, a.danji_flag, b.ym, a.made_year, a.area_type
				 , round(sum(a.unit_price*a.cnt)/sum(a.cnt), 2) unit_price, round(sum(a.price*a.cnt)/sum(a.cnt), 2) price, sum(a.cnt) cnt
              from tmp_ym b, apt_sale_stats a
             where b.ym between '202101' and '202105'
			   and a.ym between date_format(date_sub(str_to_date(concat(a.ym,'01'), '%Y%m%d'), interval 11 month), '%Y%m') and a.ym
			   and a.danji_flag = 'Y'
             group by b.ym, a.region_key, a.level, a.danji_flag, a.made_year, a.area_type
          ) a;

set @seq = @seq + 1;
insert into batch_log values(null, @seq, @job_title, now(), 'apt_qbox_stats 1 3');
delete from apt_qbox_stats where danji_flag = 'Y';
SET GROUP_CONCAT_MAX_LEN = 4294967295;
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
			and m.danji_flag = 'Y'
           and r.region_key = m.region_key
           and m.id = a.apt_id
	 group by a.ym, r.region_key, r.level, ifnull(danji_flag, 'N')) a;

set @seq = @seq + 1;
insert into batch_log values(null, @seq, @job_title, now(), 'apt_qbox_stats 1 2');
insert into apt_qbox_stats
select region_key, level, '1', danji, a.ym
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
			and m.danji_flag = 'Y'
           and r.region_key = r1.upper_region
           and r1.region_key = m.region_key
           and m.id = a.apt_id
	 group by a.ym, r.region_key, r.level, ifnull(danji_flag, 'N')) a;


set @seq = @seq + 1;
insert into batch_log values(null, @seq, @job_title, now(), 'apt_qbox_stats 1 1');
insert into apt_qbox_stats
select region_key, level, '1', danji, a.ym
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
			and m.danji_flag = 'Y'
           and r.region_key = r1.upper_region
           and r1.region_key = r2.upper_region
           and r2.region_key = m.region_key
           and m.id = a.apt_id
	 group by a.ym, r.region_key, r.level, ifnull(danji_flag, 'N')) a;


set @seq = @seq + 1;
insert into batch_log values(null, @seq, @job_title, now(), 'apt_qbox_stats 1 0');
insert into apt_qbox_stats
select region_key, level, '1', danji, a.ym
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
			and m.danji_flag = 'Y'
	 group by a.ym, r.region_key, r.level, ifnull(danji_flag, 'N')) a;


set @seq = @seq + 1;
insert into batch_log values(null, @seq, @job_title, now(), 'apt_qbox_stats 2 3');
insert into apt_qbox_stats
        select region_key, level, '2', danji, a.ym
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
			and m.danji_flag = 'Y'
           and r.region_key = m.region_key
           and m.id = a.apt_id
	 group by a.ym, r.region_key, r.level, ifnull(danji_flag, 'N')) a;


set @seq = @seq + 1;
insert into batch_log values(null, @seq, @job_title, now(), 'apt_qbox_stats 2 2');
insert into apt_qbox_stats
        select region_key, level, '2', danji, a.ym
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
			and m.danji_flag = 'Y'
           and r.region_key = r1.upper_region
           and r1.region_key = m.region_key
           and m.id = a.apt_id
	 group by a.ym, r.region_key, r.level, ifnull(danji_flag, 'N')) a;


set @seq = @seq + 1;
insert into batch_log values(null, @seq, @job_title, now(), 'apt_qbox_stats 2 1');
insert into apt_qbox_stats
        select region_key, level, '2', danji, a.ym
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
			and m.danji_flag = 'Y'
           and r.region_key = r1.upper_region
           and r1.region_key = r2.upper_region
           and r2.region_key = m.region_key
           and m.id = a.apt_id
	 group by a.ym, r.region_key, r.level, ifnull(danji_flag, 'N')) a;


set @seq = @seq + 1;
insert into batch_log values(null, @seq, @job_title, now(), 'apt_qbox_stats 2 0');
insert into apt_qbox_stats
        select region_key, level, '2', danji, a.ym
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
			and m.danji_flag = 'Y'
	 group by a.ym, r.region_key, r.level, ifnull(danji_flag, 'N')) a;
