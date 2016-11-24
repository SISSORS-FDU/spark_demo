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
val dataFrame = sparkSession.read.json("persons.json")
dataFrame.createOrReplaceTempView("person")

// 执行SparkSQL语句，显示结果
val sqlDF = sparkSession.sql("SELECT * FROM person WHERE age<20")
sqlDF.show()    
```
```
// person.json
{"name":"Michael"}
{"name":"Andy", "age":30}
{"name":"Justin", "age":19}
```

```
// output
```

## SparkSQL执行流程

![SparkSQL流程](http://sissors.cn:8080/static/sparksql_process.png)

`SparkSession.sql(sqlText)` 执行sql语句的入口

- 流程：
SQL Statement → Parse SQL → Analyze Logical Plan → Optimize Logical Plan → Generate Physical Plan → Prepareed Spark Plan → Execute SQL → Generate RDD
- 组件：
  - SparkSqlParser
    - SqlBaseLexer
    - SqlBaseParser
    - ASTBuilder
  - Analyzer
  - Optimizer
  - LogicalPlan
  - SparkPlan

### SQL Paser

`VariableSubstitution.substitute(sqlText)` 对`${var}`、`${system:var}`、`${env:var}`等语法进行解析，正则表达式匹配

`ANTLRNoCaseStringStream(sqlText)` 将Token Stream全部转为大写字母

`SqlBaseLexer` 分析sqlText的词法

`SqlBaseParser` 语法分析，将sqlText转化为ParseTree

`visitSingleStatement` singleStatement是sql语句的根节点

### ANTLR4

![Sql转化过程](http://sissors.cn:8080/static/parse_tree.png)

```
$ antlr4 Expr.g4  
$ javac *.java  
$ grun Expr prog -tokens / -gui
```

![ANTLR Listener](http://sissors.cn:8080/static/parse_tree_listeners.png)
ANTLR在它的运行库中为两种树遍历机制提供支持。默认情况下，ANTLR生成一个语法分析树Listener接口，在其中定义了回调方法，用于响应被内建的树遍历器触发的事件。
![ANTLR Listener](http://sissors.cn:8080/static/parse_tree_visitors.png)
在Listener和Visitor机制之间最大的不同是：Listener方法被ANTLR提供的遍历器对象调用；而Visitor方法必须显式的调用visit方法遍历它们的子节点，在一个节点的子节点上如果忘记调用visit方法就意味着那些子树没有得到访问。

### SQL Analyzer

> 解析SQL Paser生成的Unresolved Logical Plan（包含UnresolvedRelation、 UnresolvedFunction、 UnresolvedAttribute）

- FixedPoint：相当于迭代次数的上限
- Batch: 批次，这个对象是由一系列Rule组成的，采用一个策略
- Rule：理解为一种规则，这种规则会应用到Logical Plan 从而将UnResolved 转变为Resolved
- Strategy：最大的执行次数，如果执行次数在最大迭代次数之前就达到了fix point，策略就会停止，不再应用了

Analyzer解析主要是根据这些Batch里面定义的策略和Rule来对Unresolved的逻辑计划进行解析的。这里Analyzer类本身并没有定义执行的方法，而是要从它的父类RuleExecutor[LogicalPlan]寻找。 具体的执行方法定义在apply里：可以看到这里是一个while循环，每个batch下的rules都对当前的plan进行作用，这个过程是迭代的，直到达到Fix Point或者最大迭代次数。

```
// SparkSQL2.0 定义的Batches
lazy val batches: Seq[Batch] = Seq(
    Batch("Substitution", fixedPoint,
      CTESubstitution,
      WindowsSubstitution,
      EliminateUnions),
    Batch("Resolution", fixedPoint,
      ResolveRelations ::
      ResolveReferences ::
      ResolveDeserializer ::
      ResolveNewInstance ::
      ResolveUpCast ::
      ResolveGroupingAnalytics ::
      ResolvePivot ::
      ResolveOrdinalInOrderByAndGroupBy ::
      ResolveMissingReferences ::
      ExtractGenerator ::
      ResolveGenerate ::
      ResolveFunctions ::
      ResolveAliases ::
      ResolveSubquery ::
      ResolveWindowOrder ::
      ResolveWindowFrame ::
      ResolveNaturalAndUsingJoin ::
      ExtractWindowExpressions ::
      GlobalAggregates ::
      ResolveAggregateFunctions ::
      TimeWindowing ::
      TypeCoercion.typeCoercionRules ++
      extendedResolutionRules : _*),
    Batch("Nondeterministic", Once,
      PullOutNondeterministic),
    Batch("UDF", Once,
      HandleNullInputsForUDF),
    Batch("FixNullability", Once,
      FixNullability),
    Batch("Cleanup", fixedPoint,
      CleanupAliases)
  )
```

举一个简单例子，ResolveRelations。
比如sql = SELECT * FROM person; person表在parse后就是一个UnresolvedRelation，通过Catalog目录来寻找当前表的结构，从而从中解析出这个表的字段，会得到一个tableWithQualifiers。（即表和字段）  

```
/**
 * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
 */
object ResolveRelations extends Rule[LogicalPlan] {
  // Catalog对象里面维护了一个tableName，Logical Plan的HashMap结果
  private def lookupTableFromCatalog(u: UnresolvedRelation): LogicalPlan = {
    try {
      catalog.lookupRelation(u.tableIdentifier, u.alias)
    } catch {
      case _: NoSuchTableException =>
        u.failAnalysis(s"Table or view not found: ${u.tableName}")
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case i @ InsertIntoTable(u: UnresolvedRelation, parts, child, _, _) if child.resolved =>
      i.copy(table = EliminateSubqueryAliases(lookupTableFromCatalog(u)))
    case u: UnresolvedRelation =>
      val table = u.tableIdentifier
      if (table.database.isDefined && conf.runSQLonFile &&
          (!catalog.databaseExists(table.database.get) || !catalog.tableExists(table))) {
        // If the table does not exist, and the database part is specified, and we support
        // running SQL directly on files, then let's just return the original UnresolvedRelation.
        // It is possible we are matching a query like "select * from parquet.`/path/to/query`".
        // The plan will get resolved later.
        // Note that we are testing (!db_exists || !table_exists) because the catalog throws
        // an exception from tableExists if the database does not exist.
        u
      } else {
        lookupTableFromCatalog(u)
      }
  }
}
```

- CheckAnalysis.checkAnalysis(LogicalPlan)
