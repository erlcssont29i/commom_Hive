# commom_hive

## 時間
````
select 
add_months(trunc(CURRENT_TIMESTAMP,'MM'),-5),
add_months(trunc(CURRENT_TIMESTAMP,'MM'),-1), 
add_months(trunc(CURRENT_TIMESTAMP,'MM'),0), 
add_months(trunc(CURRENT_TIMESTAMP,'MM'),1),
substr(add_months(trunc(CURRENT_TIMESTAMP,'MM'),1),1,7)

index	      _c0           _c1       _c2         _c3         _c4
1	    2018-09-01	2019-01-01	2019-02-01	2019-03-01	2019-03
````
````
presto 用法：select substr(cast(current_date - interval '1' month as varchar), 1, 7);
````
```
dt = date_sub(current_date,1) --昨天
```

## 列轉行
collect_set :去重列转行
collect_list :不去重列转行
```
SELECT shop
      ,concat_ws('-',collect_list(cast(shangpin_num as string)))
FROM temp_test5
GROUP BY shop;
 
shop	_c1
a	1-2-3-3
b	4-5-6-6
```
将shangpin_num中所有的元素各自作为一列
```
SELECT shop
      ,collect_list(shangpin_num)[0]
      ,collect_list(shangpin_num)[1]
      ,collect_list(shangpin_num)[2]
      ,collect_list(shangpin_num)[3]
FROM temp_test5
GROUP BY shop;
 
shop	_c1	_c2	_c3	_c4
a	1	2	3	3
b	4	5	6	6
```

## 開窗函數
```
row_number() over(partition by userId order by validVisitTime
```
-----

## 建表
```
drop table if exists  dw_jdy.dws_service_login_stats_d ;
create table if not exists  dw_jdy.dws_service_login_stats_d
(

)
comment ''
partitioned BY (  par string)  
stored as orc
;

insert overwrite TABLE dw_jdy.dws_service_login_stats_d   partition (dt='${dt}')
```


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







