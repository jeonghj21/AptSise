nohup mysqldump -ujhj -p1111 dev apt_sale_items apt_master apt_sale_stats apt_ma_new apt_region_ma apt_qbox_stats raw_data_new naver_complex_info > dump_v0.9.1.sql  1>/dev/null 2>&1 &
