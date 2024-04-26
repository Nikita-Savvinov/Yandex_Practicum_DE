-- Шаг 1. Подключение к БД
-- Подключение к БД выполнено, все таблицы и представления имеются.

-- Шаг 2. Изучение новых данных.
-- для начала посмотрим на данные в таблице "external_source.craft_products_orders"
select * from external_source.craft_products_orders cpo;
-- из таблицы видно, что данные, которые в ней содержатся можно перенести в таблицы _d_craftsman, d_product, f_order
-- посмотрим на повторяющиеся значения:
select id, craftsman_id,COUNT(*)
from external_source.craft_products_orders cpo
group by id, craftsman_id
having count(*)>1;
-- вывод: повторяющиеся значения отсутствуют
-- проверим аномалии
select 
	max(cpo.craftsman_birthday)
	,min(cpo.craftsman_birthday)
	,max(cpo.product_price)
	,min(cpo.product_price)
from external_source.craft_products_orders cpo;
--аномальные значения отсутсвуют.
-- изучим таблицу customers
select * from external_source.customers c;
-- данных достаточно для переноса в таблицу d_customer
-- посмотрим на повторяющиеся значения:
select c.customer_id
		,c.customer_name ,COUNT(*)
from external_source.customers c 
group by 1, 2
having count(*)>1;
-- вывод: повторяющиеся значения отсутствуют
-- проверим аномалии
select 
	max(c.customer_birthday)
	,min(c.customer_birthday)
from external_source.customers c;
--аномальные значения отсутсвуют.

-- Шаг 3. Напишите скрипт переноса данных из источника в хранилище
-- назовем источник "es" и создадим временную таблицу измерений.
drop table if exists tmp_es;
create temp table tmp_es as
select * 
from external_source.craft_products_orders cpo
join external_source.customers c using(customer_id); 
/* обновление существующих записей и добавление новых в dwh.d_craftsmans */
MERGE INTO dwh.d_craftsman d
USING (SELECT DISTINCT craftsman_name, craftsman_address, craftsman_birthday, craftsman_email FROM tmp_es) t
ON d.craftsman_name = t.craftsman_name AND d.craftsman_email = t.craftsman_email
WHEN MATCHED THEN
  UPDATE SET 
  craftsman_address = t.craftsman_address 
  ,craftsman_birthday = t.craftsman_birthday
  ,load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm)
  VALUES (t.craftsman_name, t.craftsman_address, t.craftsman_birthday, t.craftsman_email, current_timestamp);
/* обновление существующих записей и добавление новых в dwh.d_products */
MERGE INTO dwh.d_product d
USING (SELECT DISTINCT product_name, product_description, product_type, product_price from tmp_es) t
ON d.product_name = t.product_name AND d.product_description = t.product_description AND d.product_price = t.product_price
WHEN MATCHED THEN
  UPDATE SET product_type= t.product_type, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (product_name, product_description, product_type, product_price, load_dttm)
  VALUES (t.product_name, t.product_description, t.product_type, t.product_price, current_timestamp);
/* обновление существующих записей и добавление новых в dwh.d_customer */
MERGE INTO dwh.d_customer d
USING (SELECT DISTINCT customer_name, customer_address, customer_birthday, customer_email from tmp_es) t
ON d.customer_name = t.customer_name AND d.customer_email = t.customer_email
WHEN MATCHED THEN
  UPDATE SET customer_address= t.customer_address, 
customer_birthday= t.customer_birthday, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (customer_name, customer_address, customer_birthday, customer_email, load_dttm)
  VALUES (t.customer_name, t.customer_address, t.customer_birthday, t.customer_email, current_timestamp);
 -- создадим временную таблицу фактов.
drop table if exists tmp_esf;
create temp table tmp_esf as
select  product_id
        ,craftsman_id
        ,customer_id
        ,order_created_date
        ,order_completion_date
        ,order_status
        ,current_timestamp
from external_source.craft_products_orders cpo; 
/* обновление существующих записей и добавление новых в dwh.f_order */
MERGE INTO dwh.f_order f
USING tmp_esf t
ON f.product_id = t.product_id AND f.craftsman_id = t.craftsman_id AND f.customer_id = t.customer_id AND f.order_created_date = t.order_created_date 
WHEN MATCHED THEN
  UPDATE SET order_completion_date = t.order_completion_date, order_status = t.order_status, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, load_dttm)
  VALUES (t.product_id, t.craftsman_id, t.customer_id, t.order_created_date, t.order_completion_date, t.order_status, current_timestamp); 
-- Шаг 4. создание таблицы customer_report_datamart
 drop table if exists dwh.customer_report_datamart;
 create table if not exists dwh.customer_report_datamart (
 	id int generated always as identity not null
 	,customer_id int not null
 	,customer_name text not null
 	,customer_address text not null
 	,customer_birthday timestamp not null
 	,customer_email text not null
 	,customer_money numeric (15,2) not null
 	,platform_money int not null
 	,count_order int not null
 	,avg_price_order numeric (10,2) not null
 	,median_time_order_completed numeric(10,1) 
 	,top_product_category varchar not null
 	,top_craftsman_id int not null
 	,count_order_created int not null
 	,count_order_in_progress int not null
 	,count_order_delivery int not null
 	,count_order_done int not null
 	,count_order_not_done int not null
 	,report_period varchar not null
 	,constraint customer_report_datamart_pk primary key (id)
 	);
 
-- Шаг 5. Напишите DDL новой витрины
 --- ШАГ 1 Создание дополнительной таблицы
 drop table if exists dwh.load_dates_customer_report_datamart;
 create table if not exists dwh.load_dates_customer_report_datamart (
 	id int generated always as identity not null
 	,load_dttm date not null
 	,constraint load_dates_customer_report_datamart_pk primary key (id)
 );
 --- ШАГ 2 Выбор из хранилища только измененные или новые данные
with 
dwh_delta as ( 
	select 
		fo.customer_id as customer_id
		,dc.customer_name as customer_name 
		,dc.customer_address as customer_address 
		,dc.customer_birthday as customer_birthday
		,dc.customer_email as customer_email 
		,fo.order_id as order_id
		,dp.product_id as product_id
        ,dp.product_price as product_price
        ,dp.product_type as product_type
        ,fo.order_completion_date - fo.order_created_date as diff_order_date
        ,fo.order_status as order_status
        ,to_char (fo.order_created_date, 'yyyy-mm') as report_period
        ,crd.customer_id as exist_customer_id
        ,fo.craftsman_id as craftsman_id
        ,dc.load_dttm as customers_load_dttm
        ,dc2.load_dttm as craftsman_load_dttm
        ,dp.load_dttm as products_load_dttm
	from dwh.f_order fo
	INNER JOIN dwh.d_customer dc ON fo.customer_id = dc.customer_id
   	inner join dwh.d_product dp on fo.product_id = dp.product_id 
   	inner join dwh.d_craftsman dc2 on fo.order_id = dc2.craftsman_id 
    left join dwh.customer_report_datamart crd on fo.customer_id = crd.id
   	where fo.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart))
--select * from dwh_delta; 
--- ШАГ 3 Определить, какие данные из дельты нужно обновить
,dwh_update_delta as (
	select
		customer_id
		from dwh_delta
			where exist_customer_id = null
)
--- Шаг 4. Выполнить расчёт витрины только для данных, которые нужно вставить
,dwh_delta_insert_result as (
	select
		T4.customer_id as customer_id 
 		,T4.customer_name as customer_name  
	 	,T4.customer_address as customer_address  
	 	,T4.customer_birthday as customer_birthday 
	 	,T4.customer_email as customer_email 
	 	,T4.customer_money as customer_money
	 	,T4.platform_money as platform_money 
	 	,T4.count_order as count_order
	 	,T4.avg_price_order as avg_price_order 
	 	,T4.median_time_order_completed as median_time_order_completed 
	 	,T4.product_type as top_product_category 
	 	,T4.craftsman_id as top_craftsman_id
	 	,T4.count_order_created as count_order_created
	 	,T4.count_order_in_progress as count_order_in_progress
	 	,T4.count_order_delivery as count_order_delivery
	 	,T4.count_order_done as count_order_done 
	 	,T4.count_order_not_done as count_order_not_done 
	 	,T4.report_period as report_period
	 from (
	 	select
	 	*
	 	,RANK() OVER(PARTITION BY T2.customer_id ORDER BY count_product DESC) AS rank_count_product
	 	,rank() over(partition by T2.customer_id order by craftsman_id desc) as rank_craftsman_id
	 	from (
			select 
				T1.customer_id as customer_id 
		 		,T1.customer_name as customer_name  
			 	,T1.customer_address as customer_address  
			 	,T1.customer_birthday as customer_birthday 
			 	,T1.customer_email as customer_email 
				,SUM(T1.product_price) as customer_money
				,SUM(T1.product_price)*0.1 as platform_money
				,COUNT (T1.order_id) as count_order	
				,AVG(T1.product_price) as avg_price_order
				,percentile_cont(0.5) within group (order by T1.diff_order_date) as median_time_order_completed
				,sum (case when T1.order_status = 'created' then 1 else 0 end) as count_order_created
				,sum (case when T1.order_status = 'in progress' then 1 else 0 end) as count_order_in_progress
				,sum (case when T1.order_status = 'delivery' then 1 else 0 end) as count_order_delivery
				,sum (case when T1.order_status = 'done' then 1 else 0 end) as count_order_done
				,sum (case when T1.order_status != 'done' then 1 else 0 end) as count_order_not_done
				,T1.report_period
			from dwh_delta T1
				where T1.exist_customer_id is null
				group by customer_id,customer_name,customer_address,customer_birthday,customer_email,report_period) as T2
			inner join -- расчет топ категории
				(select 
					dd.customer_id as customer_id_for_product_type
					,dd. product_type
					,COUNT (dd. product_type) as count_product
				from dwh_delta dd 
				group by dd.customer_id 
						,dd. product_type
				ORDER BY count_product desc) as T3
			on T2.customer_id =T3.customer_id_for_product_type
			inner join -- расчет топ мастера
				(select 
					customer_id as customer_id_for_craftsman 
					,craftsman_id 
					,count (craftsman_id) as count_craftsman
				from dwh_delta	
				group by customer_id
						,craftsman_id
				order by count_craftsman desc) as T5		
			on T2.customer_id =T5.customer_id_for_craftsman) as T4
	where T4.rank_count_product = 1 
		and T4.rank_craftsman_id >=1
	ORDER BY report_period
	 	)	
--select * from  	dwh_delta_insert_result
--- Шаг 5. Выполнить расчёт витрины для данных, которые нужно обновить
-- создаём таблицу dwh_delta_update_result: делаем перерасчёт для существующих записей витрины данных, так как данные обновились за отчётные периоды 
-- логика похожа на insert, но нужно достать конкретные данные из DWH
,dwh_delta_update_result as (
	select
			T4.customer_id as customer_id 
	 		,T4.customer_name as customer_name  
		 	,T4.customer_address as customer_address  
		 	,T4.customer_birthday as customer_birthday 
		 	,T4.customer_email as customer_email 
		 	,T4.customer_money as customer_money
		 	,T4.platform_money as platform_money 
		 	,T4.count_order as count_order
		 	,T4.avg_price_order as avg_price_order 
		 	,T4.median_time_order_completed as median_time_order_completed 
		 	,T4.product_type as top_product_category 
		 	,T4.craftsman_id as top_craftsman_id
		 	,T4.count_order_created as count_order_created
		 	,T4.count_order_in_progress as count_order_in_progress
		 	,T4.count_order_delivery as count_order_delivery
		 	,T4.count_order_done as count_order_done 
		 	,T4.count_order_not_done as count_order_not_done 
		 	,T4.report_period as report_period
		 from (
		 	select
		 	*
		 	,RANK() OVER(PARTITION BY T2.customer_id ORDER BY count_product DESC) AS rank_count_product
		 	,rank() over(partition by T2.customer_id order by craftsman_id desc) as rank_craftsman_id
		 	from (
				select 
					T1.customer_id as customer_id 
			 		,T1.customer_name as customer_name  
				 	,T1.customer_address as customer_address  
				 	,T1.customer_birthday as customer_birthday 
				 	,T1.customer_email as customer_email 
					,SUM(T1.product_price) as customer_money
					,SUM(T1.product_price)*0.1 as platform_money
					,COUNT (T1.order_id) as count_order	
					,AVG(T1.product_price) as avg_price_order
					,percentile_cont(0.5) within group (order by T1.diff_order_date) as median_time_order_completed
					,sum (case when T1.order_status = 'created' then 1 else 0 end) as count_order_created
					,sum (case when T1.order_status = 'in progress' then 1 else 0 end) as count_order_in_progress
					,sum (case when T1.order_status = 'delivery' then 1 else 0 end) as count_order_delivery
					,sum (case when T1.order_status = 'done' then 1 else 0 end) as count_order_done
					,sum (case when T1.order_status != 'done' then 1 else 0 end) as count_order_not_done
					,T1.report_period
				from ( 
					select -- в этой выборке достаём из DWH обновлённые или новые данные по мастерам, которые уже присутствуют в витрине
						dcs.customer_id as customer_id 
				 		,dcs.customer_name as customer_name  
					 	,dcs.customer_address as customer_address  
					 	,dcs.customer_birthday as customer_birthday 
					 	,dcs.customer_email as customer_email 
						,fo.order_id AS order_id
						,dp.product_id AS product_id
						,dp.product_price AS product_price
						,dp.product_type AS product_type
						,fo.order_completion_date - fo.order_created_date AS diff_order_date
						,fo.order_status AS order_status
						,to_char (fo.order_created_date, 'yyyy-mm') AS report_period
						FROM dwh.f_order fo 
							INNER JOIN dwh.d_customer dcs ON fo.customer_id = dcs.customer_id
							INNER JOIN dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id 
							INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id
							INNER JOIN dwh_update_delta ud ON fo.customer_id = ud.customer_id
					) as T1	
					group by customer_id,customer_name,customer_address,customer_birthday,customer_email,report_period) as T2
					inner join -- расчет топ категории
						(select 
							dd.customer_id as customer_id_for_product_type
							,dd. product_type
							,COUNT (dd. product_type) as count_product
						from dwh_delta dd 
						group by dd.customer_id 
								,dd. product_type
						ORDER BY count_product desc) as T3
					on T2.customer_id =T3.customer_id_for_product_type
					inner join -- расчет топ мастера
						(select 
							customer_id as customer_id_for_craftsman 
							,craftsman_id 
							,count (craftsman_id) as count_craftsman
						from dwh_delta	
						group by customer_id
								,craftsman_id
						order by count_craftsman desc) as T5		
					on T2.customer_id =T5.customer_id_for_craftsman) as T4
			where T4.rank_count_product = 1 
				and T4.rank_craftsman_id >=1
			ORDER BY report_period
)
--select * from dwh_delta_update_result
--- Шаг 6. Выполнить вставку новых данных в витрину
,insert_delta as (
	insert into dwh.customer_report_datamart (
	customer_id 
	,customer_name 
 	,customer_address 
 	,customer_birthday 
 	,customer_email  
 	,customer_money 
 	,platform_money  
 	,count_order 
 	,avg_price_order  
 	,median_time_order_completed  
 	,top_product_category  
 	,top_craftsman_id 
 	,count_order_created 
 	,count_order_in_progress 
 	,count_order_delivery 
 	,count_order_done  
 	,count_order_not_done  
 	,report_period)
 		select
 			customer_id 
			,customer_name 
		 	,customer_address 
		 	,customer_birthday 
		 	,customer_email  
		 	,customer_money 
		 	,platform_money  
		 	,count_order 
		 	,avg_price_order  
		 	,median_time_order_completed  
		 	,top_product_category  
		 	,top_craftsman_id 
		 	,count_order_created 
		 	,count_order_in_progress 
		 	,count_order_delivery 
		 	,count_order_done  
		 	,count_order_not_done  
		 	,report_period
		 	from dwh_delta_insert_result		
)
--- Шаг 7. Выполнить обновление изменённых данных в витрине
,update_delta as (
	update dwh.customer_report_datamart set
	customer_id = upd.customer_id
	,customer_name = upd.customer_name
 	,customer_address = upd.customer_address
 	,customer_birthday = upd.customer_birthday
 	,customer_email = upd.customer_email
 	,customer_money = upd.customer_money
 	,platform_money  = upd.platform_money
 	,count_order = upd.count_order
 	,avg_price_order = upd.avg_price_order
 	,median_time_order_completed = upd.median_time_order_completed 
 	,top_product_category = upd.top_product_category
 	,top_craftsman_id = upd.top_craftsman_id
 	,count_order_created = upd.count_order_created
 	,count_order_in_progress = upd.count_order_in_progress
 	,count_order_delivery = upd.count_order_delivery
 	,count_order_done = upd.count_order_done 
 	,count_order_not_done = upd.count_order_not_done 
 	,report_period = upd.report_period
 	from (select
 			customer_id 
			,customer_name 
		 	,customer_address 
		 	,customer_birthday 
		 	,customer_email  
		 	,customer_money 
		 	,platform_money  
		 	,count_order 
		 	,avg_price_order  
		 	,median_time_order_completed  
		 	,top_product_category  
		 	,top_craftsman_id 
		 	,count_order_created 
		 	,count_order_in_progress 
		 	,count_order_delivery 
		 	,count_order_done  
		 	,count_order_not_done  
		 	,report_period
		 from dwh_delta_update_result) as upd
		 where dwh.customer_report_datamart.customer_id = upd.customer_id 	
)
--- Шаг 8. Выполнить вставку максимальной даты загрузки из дельты в дополнительную таблицу
,insert_load_date AS ( -- делаем запись в таблицу загрузок о том, когда была совершена загрузка, чтобы в следующий раз взять данные, которые будут добавлены или изменены после этой даты
    INSERT INTO dwh.load_dates_customer_report_datamart (
        load_dttm
    )
    SELECT GREATEST(COALESCE(MAX(craftsman_load_dttm), NOW()), 
                    COALESCE(MAX(customers_load_dttm), NOW()), 
                    COALESCE(MAX(products_load_dttm), NOW())) 
        FROM dwh_delta
)
SELECT 'increment datamart';
