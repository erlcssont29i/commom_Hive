# commom_SQL

select add_months(trunc(CURRENT_TIMESTAMP,'MM'),-5),
add_months(trunc(CURRENT_TIMESTAMP,'MM'),-1), 
add_months(trunc(CURRENT_TIMESTAMP,'MM'),0), 
add_months(trunc(CURRENT_TIMESTAMP,'MM'),1),
substr(add_months(trunc(CURRENT_TIMESTAMP,'MM'),1),1,7)

index	  _c0           _c1       _c2         _c3         _c4
1	    2018-09-01	2019-01-01	2019-02-01	2019-03-01	2019-03
