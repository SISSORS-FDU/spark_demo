## spark收藏夹
> http://www.one-tab.com/page/Z-A3VA7mQkK6-vHIz7AoSw

> http://abbeychenxi.net/sql-parser-in-antlr/

> http://dataunion.org/21057.html

## maven plugin配置：
> https://my.oschina.net/zh119893/blog/276090

> http://www.cnblogs.com/crazy-fox/archive/2012/02/09/2343722.html

# SparkSQL 2.0源码分析

## Spark生态系统
![Spark框架图](http://sissors.cn:8080/static/spark_ecosystem.JPG)

## SparkSQL示例
```
// 创建SparkSession
val sparkSession = SparkSession
    .builder()
    .master("local")
    .appName("SparkSQL")
    .getOrCreate()

// 通过读取文件获取DataFrame，创建person表
val dataFrame = sparkSession.read.json("spark_sql\\src\\main\\resources\\persons.json")
dataFrame.createOrReplaceTempView("person")

// 执行SparkSQL语句，显示结果
val sqlDF = sparkSession.sql("SELECT * FROM person WHERE age<20")
sqlDF.show()    
```

## SparkSQL执行流程

![SparkSQL流程](http://sissors.cn:8080/static/sparksql_process.png)

`SparkSession.sql(sqlText)` 执行sql语句的入口

### SQL Paser

`VariableSubstitution.substitute(sqlText)` 对`${var}`、`${system:var}`、`${env:var}`等语法进行解析，正则表达式匹配

`ANTLRNoCaseStringStream(sqlText)` 将Token Stream全部转为大写字母

`SqlBaseLexer` 提取sqlText关键字【】

`SqlBaseParser` sqlText转化为ParseTree【】

`visitSingleStatement` singleStatement是sql语句的根节点【vistXXX和SqlBase.4g的关系，详细流程】

### SQL Analyzer

> 解析SQL Paser生成的Unresolved Logical Plan（包含UnresolvedRelation、 UnresolvedFunction、 UnresolvedAttribute）

- FixedPoint：相当于迭代次数的上限
- Batch: 批次，这个对象是由一系列Rule组成的，采用一个策略【】
- Rule：理解为一种规则，这种规则会应用到Logical Plan 从而将UnResolved 转变为Resolved
- Strategy：最大的执行次数，如果执行次数在最大迭代次数之前就达到了fix point，策略就会停止，不再应用了

Analyzer解析主要是根据这些Batch里面定义的策略和Rule来对Unresolved的逻辑计划进行解析的。这里Analyzer类本身并没有定义执行的方法，而是要从它的父类RuleExecutor[LogicalPlan]寻找。 具体的执行方法定义在apply里：可以看到这里是一个while循环，每个batch下的rules都对当前的plan进行作用，这个过程是迭代的，直到达到Fix Point或者最大迭代次数。

【ResolveRelations】

checkAnalysis
