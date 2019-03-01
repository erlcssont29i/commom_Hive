# commom_SQL

select add_months(trunc(CURRENT_TIMESTAMP,'MM'),-5),
add_months(trunc(CURRENT_TIMESTAMP,'MM'),-1), 
add_months(trunc(CURRENT_TIMESTAMP,'MM'),0), 
add_months(trunc(CURRENT_TIMESTAMP,'MM'),1),
substr(add_months(trunc(CURRENT_TIMESTAMP,'MM'),1),1,7)

index	  _c0           _c1       _c2         _c3         _c4
1	    2018-09-01	2019-01-01	2019-02-01	2019-03-01	2019-03


presto 用法：select substr(cast(current_date - interval '1' month as varchar), 1, 7);

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
