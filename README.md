# commom_SQL
````
select add_months(trunc(CURRENT_TIMESTAMP,'MM'),-5),
add_months(trunc(CURRENT_TIMESTAMP,'MM'),-1), 
add_months(trunc(CURRENT_TIMESTAMP,'MM'),0), 
add_months(trunc(CURRENT_TIMESTAMP,'MM'),1),
substr(add_months(trunc(CURRENT_TIMESTAMP,'MM'),1),1,7)

index	  _c0           _c1       _c2         _c3         _c4
1	    2018-09-01	2019-01-01	2019-02-01	2019-03-01	2019-03
````
````
presto 用法：select substr(cast(current_date - interval '1' month as varchar), 1, 7);
````
-----


可以在查询前加上 "set hive.strict.checks.type.safety=false; set hive.mapred.mode=nonstrict;" .

5.hive 语法规范化
总结我们遇到过的各种hive问题，其中大半以上是由于用法不规范导致的。尤其是hive的隐式类型转换，经常导致数据结果不正确。

为了避免类似情况重复发生，倡导更规范的语法，我们决定采用更严格的语法限制，以下类型的hql语句，之前能正常执行，之后执行会报错。

使用了保留字的语句，如date、user等
报错信息如：Failed to recognize predicate 'date'. Failed rule: 'identifier'
解决办法：为关键字加上反单引号即可。请不要使用set hive.support.sql11.reserved.keywords=false，这种方法新版本的hive中已经不支持了。

union 的字段类型不匹配
报错信息如：Schema of both sides of union should match: Column id is of type int on first table and type string on second table
解决办法：使用cast 进行强制类型转换，不少同学反映和null 值union的问题，例如和int不匹配，可以改成 cast( null as int )

对不同类型的数据进行比较
报错信息如：Wrong arguments 'user_no': Unsafe compares between different types are disabled for safety reasons
解决办法：使用cast 进行强制类型转换

使用了笛卡尔积
报错信息如：Cartesian products are disabled for safety reasons.
解决办法：hive 中最好不要使用笛卡尔积，如果确定数据量不大，可以在查询前面加上
set hive.mapred.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

LENGTH()中的参数不是字符
报错信息如：LENGTH() only takes STRING/CHAR/VARCHAR/BINARY types as first argument, got INT
解决办法：这个用法就是不合理的，具体看需求
  

查询分区表未选择分区
报错信息：Queries against partitioned tables without a partition filter are disabled for safety reasons
解决办法：尽量指定分区，如果确实要这么做，可以在查询前面加上
set hive.mapred.mode=nonstrict;
set hive.strict.checks.large.query=false;

order by 不带limit
报错信息：Order by-s without limit are disabled for safety reasons
解决办法：可以在查询前面加上
set hive.mapred.mode=nonstrict;
set hive.strict.checks.large.query=false;

````
----排序---
--  S3商品月销量top1商品
create table  dev.Jx_deal_Stores_Order_Goods as 
select
            t2.cp_month,
            t2.kdt_id,
        	t2.goods_id,
        	t2.goods_alias,
        	t2.goods_title,
        	t2.sold_item_num,
        	t2.sold_item_gmv,
            t2.sold_item_rank

from (
select 
     t1.cp_month,
      t1.kdt_id,
	  t1.goods_id,
	   t1.goods_alias,
	   t1.goods_title,
	   t1.sold_item_num,
	   t1.sold_item_gmv,
row_number()over(partition by t1.kdt_id  , t1.cp_month order by t1.sold_item_num desc) as sold_item_rank
from
(select 
		cp_month,
         kdt_id,
         goods_id,
         goods_alias,
         goods_title,
         sum(item_num)as sold_item_num,
         sum(item_real_pay)as sold_item_gmv
 from dev.Jx_deal_Stores_Order
 where cp_month between '2018-01' and '2018-12'
 group by kdt_id,cp_month,goods_id,goods_alias,goods_title
 )t1
     )t2
 where t2.sold_item_rank =1 
;
````


--
````
@orderBy= created_time 

SELECT 
created_time
,count(distinct kdt_id) as kdt_id_num
,sum(if(diff=0,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D0
,sum(if(diff=1,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D1
,sum(if(diff=2,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D2
,sum(if(diff=3,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D3
,sum(if(diff=4,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D4
,sum(if(diff=5,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D5
,sum(if(diff=6,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D6
,sum(if(diff=7,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D7
,sum(if(diff=8,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D8
,sum(if(diff=9,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D9
,sum(if(diff=10,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D10
,sum(if(diff=11,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D11
,sum(if(diff=12,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D12
,sum(if(diff=13,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D13
,sum(if(diff=14,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D14
,sum(if(diff=15,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D15
,sum(if(diff=16,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D16
,sum(if(diff=17,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D17
,sum(if(diff=18,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D18
,sum(if(diff=19,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D19
,sum(if(diff=20,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D20
,sum(if(diff=21,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D21
,sum(if(diff=22,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D22
,sum(if(diff=23,log_pc_or_app,0))*1.0/count(distinct kdt_id) as D23
,sum(if(diff=24,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D24
,sum(if(diff=25,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D25
,sum(if(diff=26,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D26
,sum(if(diff=27,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D27
,sum(if(diff=28,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D28
,sum(if(diff=29,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D29
,sum(if(diff=30,log_pc_or_app,0))*1.000/count(distinct kdt_id) as D30

from mkt_activation_raw_data
where 1=1
and team_from in (0,1,2)
and replace(cast(created_time as varchar), '-', '')<= cast( ${date_end} as varchar)
and replace(cast(created_time as varchar), '-', '')>= cast( ${date_start} as varchar)
group by created_time
;
````

````
drop table bi.mkt_activation_raw_data;


set hive.mapred.mode=nonstrict;
set hive.strict.checks.large.query=false;


create table bi.mkt_activation_raw_data as 

select a.kdt_id as kdt_id
,a.team_type as team_type
,a.team_from   -- 注册来源：0:pc 1:app手机app 2：pad 3:weapp微信小程序 4：h5手机浏览器
,case   when a.team_from=0 then '0:pc'
    	when a.team_from=1 then '1:app手机app'
    	when a.team_from=2 then '2：pad'
    	when a.team_from=3 then '3:weapp微信小程序'
    	when a.team_from=4 then '4：h5手机浏览器' end as team_from_name 
,a.created_time as created_time
,a.open_status as open_status
,b.par as par
,b.log_app as log_app
,b.log_pc as log_pc

,case when b.log_pc=1 and b.log_app=0 then 1 else 0 end as log_pc_only --
,case when b.log_pc=0 and b.log_app=1 then 1 else 0 end as log_app_only --
,case when b.log_pc=1 and b.log_app=1 then 1 else 0 end as log_pc_and_app --
,case when b.log_pc=1 or b.log_app=1 then 1 else 0 end as log_pc_or_app --

,datediff(b.par,a.created_time) as diff
,case when datediff(b.par,a.created_time)=0 then 'day0'
    when datediff(b.par,a.created_time) in(1,2,3) then 'day123'
    when datediff(b.par,a.created_time) in (4,5,6,7) then 'day4567'
    when datediff(b.par,a.created_time)in (8,9,10,11,12,13,14,15) then'day8_15'
    when datediff(b.par,a.created_time)BETWEEN 16 and 30  then 'day16_30'
    when datediff(b.par,a.created_time)> 30 then 'day30_up'
 end as diff_range
,case when b.log_pc=1 and b.log_app=0 then 'pc_only'
when b.log_pc=0 and b.log_app=1 then 'app_only'
when b.log_pc=1 and b.log_app=1 then 'pc_and_app'
else 'no_log' end as log_type --

from(
SELECT kdt_id ,team_type,team_type_name,team_from
,to_date(created_time)as created_time
,to_date(first_trade)as first_trade,open_status from dw.dws_team_biz_d
where team_type in(0,5,6,7,9)
and team_property=0
-- and to_date(created_time) BETWEEN'2017-06-01' and substr(CURRENT_TIMESTAMP,1,10)
) a
left join 
(select kdt_id
,from_unixtime(unix_timestamp(par ,'yyyymmdd'),'yyyy-mm-dd') as par
,count( DISTINCT case when visit_type='app' then 'app' else Null end) as log_app
,count( DISTINCT case when visit_type='pc' then 'pc' else Null end) as log_pc
from dw.dwd_login_d
-- where par
group by kdt_id,par) b
on a.kdt_id=b.kdt_id
;
````
