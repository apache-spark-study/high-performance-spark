# DataFrame, Dataset, 그리고 스파크 SQL

## Spark SQL

> Spark SQL is a Spark module for structured data processing. Unlike the basic Spark RDD API, the interfaces provided by Spark SQL provide Spark with more information about the structure of both the data and the computation being performed. Internally, Spark SQL uses this extra information to perform extra optimizations. There are several ways to interact with Spark SQL including SQL and the Dataset API. - [spark official docs](https://spark.apache.org/docs/latest/sql-programming-guide.html#spark-sql-dataframes-and-datasets-guide)

**Spark SQL이란 구조화된 데이터 프로세싱을 위한 스파크가 제공하는 모듈이다.**  Spark SQL 엔진이라고도 하며, Spark 1.0에서 처음 소개되었다. Spark SQL은 말그대로 SQL을 가능하게 해주는 인터페이스 역할을 하며, 이는 hive를 포함한 다양한 storage에 접근할 수 있는 편리한 API를 제공해준다. 특히, Spark SQL의 API는 사용자에게 데이터 및 연산의 구조에 관한 다양한 정보를 제공해주며, 이 정보들은 내부적으로 (Spark SQL’s optimized execution engine 을 통한) 성능 최적화에도 도움을 준다. Spark SQL API를 사용한다면 데이터 셋을 RDD가 아닌 DataFrame/DataSet 으로 표현하게 된다.

<br>

## DataFrame
> A DataFrame is a  _Dataset_  organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in R/Python, but with richer optimizations under the hood.  - [spark official docs](https://spark.apache.org/docs/latest/sql-programming-guide.html#spark-sql-dataframes-and-datasets-guide)

- DataFrame이란 스키마 정보를 갖는 데이터 셋이다. 즉, RDBMS의 테이블과 같은 개념이다.
- Spark 1.3에서 처음 소개되었다.
- Spark 2.0에서 Dataset API에 포함되었다.


**RDD VS DataFrame**
- RDD처럼 변경 불가능한 분산 데이터 셋을 표현한다.
- RDD와는 다르게 스키마 정보를 가지고 있다.
- 데이터 타입을 다루는 방식의 차이.
  - RDD: 내부 데이터의 타입을 명확하게 정의해서 사용하도록 강제. e.g) RDD[String].map(x => x.charAt() ) - O
  -   DataFrame: 내부 데이터가 Row의 집합이라는 것만 보장돼 있을 뿐 실제 데이터의 타입에 대한 정보는 외부에 노출돼 있지 않음.  e.g) DataSet[Row].map(x => x.charAt() ) - X

<br>

## DataSet
> A Dataset is a distributed collection of data. Dataset is a new interface added in Spark 1.6 that provides the benefits of RDDs (strong typing, ability to use powerful lambda functions) with the benefits of Spark SQL’s optimized execution engine. - [spark official docs](https://spark.apache.org/docs/latest/sql-programming-guide.html#spark-sql-dataframes-and-datasets-guide)

- Spark 1.6에서 처음 도입되었다.
- From Spark 2.0.X, Spark SQL에서 사용하는 유일한 분산 데이터 모델이다.

**RDD가 있음에도 DataFrame / DataSet이 새롭게 등장한 이유**
  - 값뿐만 아니라 스키마 정보까지 함께 포함하기 때문에 스키마를 기반으로 한 데이터 처리 가능
  - 내부적인 성능 최적화를 함께 제공할 수 있음. ( 최적화를 통한 성능 개선의 가능성을 제공 )

**DataFrame과의 구분**
  - DataSet[T]
  - DataSet[Row]: DataFrame ( Spark 2.0부터 Row 타입을 갖는 데이터셋을 가리키는 용어로 사용되고 있음 )
- DataSet의 Transformation은 데이터 타입을 처리하는 방법에 따라 typed operation / untyped operation 으로 구분.
  - typed operation: DataSet[Int], DataSet[String] ..
  - untyped operation: DataSet[Row]
    - 데이터를 처리할 때, 본래의 타입이 아닌 Column 타입의 객체로 감싸서 처리하는 연산.        
    - 즉, DataSet[Row] ( DataFrame ) 은 원래 데이터 타입 정보를 사용하지 않는 연산인 untyped operations에 속한다고 할 수 있음.

<br>

## SparkSession으로 시작하기
- SparkSession은 SparkSQL의 시작점을 제공한다.
- SparkSession은 Builder Pattern으로 생성되며 getOrCreate() 가 build()의 역할을 한다. 참고로 이미 세션이 존재하는 경우, SparkSession 생성시 직접 설정한 값을 모두 무시하고, 존재하는 SparkSession을 가져오기만 한다.
- Spark 2.0 이전에는 Spark SQL을 사용할 때 SparkSession 대신 두 가지의 다른 진입점이 존재했다. HiveContext 혹은 SQLContext 가 그것이다.
```scala
// 참고로 spark-shell 에서는 spark이라는 변수명으로 SparkSession이 이미 정의되어 있다.
// e.g) val spark: org.apache.spark.sql.SparkSession

import spark.implicits._
import org.apache.spark.sql.functions._

val d1 = ("store2", "note", 20, 2000)
val d2 = ("store2", "bag", 10, 5000)
val d3 = ("store1", "note", 15, 1000)
val d4 = ("store1", "pen", 20, 5000)

val data = Seq(d1, d2, d3,d4)
val df = data.toDF("store", "product", "amount", "price")
```

<br>

## Schema 기초
스키마의 정보와 그로 인해 가능해지는 최적화는 스파크 SQL과 코어 스파크 사이의 핵심 차이점 중의 하나다. 스키마는 보통 스파크 SQL에 의해 자동으로 처리되며, 데이터를 로딩할 때 스키마 추측이 이루어지거나 부모 DataFrame과 적용된 트랜스포메이션에 의해 계산된다.  
DataFrame은 사람이 읽기 편한 형식인 printSchema()와, 프로그래밍을 위한 형식 schema() 으로 스키마 정보를 제공한다. schema()를 호출하면 StructType, StructField 등이 결과로 반환된다. StructType은 필드들의 리스트를 갖는다. 리스트의 요소로 StructType이 또 사용될 수 있다. StructType안의 필드들은 
```scala
scala> df.printSchema
root
 |-- store: string (nullable = true)
 |-- product: string (nullable = true)
 |-- amount: integer (nullable = false)
 |-- price: integer (nullable = false)

scala> df.schema
res1: org.apache.spark.sql.types.StructType = StructType(StructField(store,StringType,true), StructField(product,StringType,true), StructField(amount,IntegerType,false), StructField(price,IntegerType,false))
```
_Spark SQL Schema는 그 내부의 데이터와 달리 즉시 평가된다. 만약 shell에서 특정 transformation이 어떻게 동작할지  예측이 안된다면 바로 스키마를 찍어볼 수 있다._
```scala
scala> df.drop("store") // transformation
res7: org.apache.spark.sql.DataFrame = [product: string, amount: int ... 1 more field]

scala> res7.printSchema
root
 |-- product: string (nullable = true)
 |-- amount: integer (nullable = false)
 |-- price: integer (nullable = false)
```

## DataFrame API
스파크 SQL의 DataFrame API는 임시 테이블을 등록하거나 SQL 표현을 생성하지 않고도 DataFrame으로 작업할 수 있게 해준다. DataFrame API는 트랜스포메이션과 액션을 모두 갖고 있다.  

### 트랜스포메이션
DataFrame의 트랜스포메이션은 제한된 표현을 사용하므로 옵티마이저는 더 많은 정보를 얻을 수 있다. RDD와 마찬가지로 트랜스포메이션 또한 단일 DataFrame, 다중 DataFrame, Key/Value Pair, Grouping/Window 등으로 분류할 수 있다.

**단순한 DataFrame 트랜스포메이션과 SQL 표현들**  
```scala
/* filter example */
scala> df.filter(df("store") === "store1")
res16: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [store: string, product: string ... 3 more fields]

// 아래와 같은 복합적인 표현식은 저장계층까지 밀어 내리기(filter push down)는 좀 더 어려우므로 단순한 필터를 쓴 RDD에서 보던 것에 비해 속도 향상은 크지 않을 수 있다.
// 그 이유는 필터에 사용하는 필드가 많아질수록 dependency 가 늘어나기 때문이라고 생각한다.
scala> df.filter((df("store") === "store1").and(df("amount") > 17))
res18: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [store: string, product: string ... 3 more fields]
```

```scala
/* explode example */
```

**유실 데이터나 문제 데이터에 대한 특수한 DataFrame 트랜스포메이션**  
유실 데이터나 null, 잘못된 데이터를 다룰 수 있는 특수한 도구들을 제공한다. 필터와 함께 isNaN이나 isNull을 써서 필요한 레코드를 가져오기 위한 조건을 만들 수 있다.
```scala
import spark.implicits._
import org.apache.spark.sql.functions._

val d1 = ("store2", null, 20, 2000)
val d2 = ("store2", null, 10, 5000)
val d3 = ("store1", null, 15, 1000)
val d4 = ("store1", "pen", 20, 5000)

val data = Seq(d1, d2, d3,d4)
val df = data.toDF("store", "product", "amount", "price")


// def coalesce(e: org.apache.spark.sql.Column*): org.apache.spark.sql.Column
// null이 아닌 첫번째 컬럼을 반환한다.
scala> coalesce(df("product"))
res20: org.apache.spark.sql.Column = coalesce(product)
```

**행 단위 변환 방식을 넘어**  
때에 따라 filter로 했던 일 같은 행 단위 방식을 적용하는 것이 충분하지 않을 수도 있다. Spark SQL은 dropDuplicates 같은 함수를 써서 중복되지 않는 레코드만 뽑아낼 수도 있다. _RDD에서 이런 유의 연산(distinct)은 셔플을 요구하게 되고 종종 filter를 쓰는 것보다 훨씬 느리다._ 이에 반해 dropDuplicates()는 특정 필드만 선택하는 식으로 컬럼 중 일부분만을 대상으로 하여 선택적으로 데이터의 일부를 걷어낼 수 있다.  

```scala
scala> df.show(false)
+------+-------+------+-----+
|store |product|amount|price|
+------+-------+------+-----+
|store2|null   |20    |2000 |
|store2|null   |10    |5000 |
|store1|null   |15    |1000 |
|store1|pen    |20    |5000 |
+------+-------+------+-----+

scala> df.dropDuplicates("store")
res43: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [store: string, product: string ... 2 more fields]

scala> res43.show(false)
+------+-------+------+-----+
|store |product|amount|price|
+------+-------+------+-----+
|store2|null   |20    |2000 |
|store1|null   |15    |1000 |
+------+-------+------+-----+
```

**집계 연산과 groupBy**  
Spark SQL은 여러 강력한 집계 연산 (aggregation)이 가능하며 옵티마이저 덕택에 여러 번의 집계 연산을 단일 액션이나 질의로 합쳐 주기도 한다. RDD에 익숙하다면 groupBy에 대해 우려가 생길 수도 있지만, 현재는 Spark SQL이 자동으로 파이프라인을 간략화해주고 규모가 큰 셔플과 레코드 처리를 회피해 주는 덕택에 안전하게 쓸 수 있다.
```scala
scala> df.show(false)
+------+-------+------+-----+
|store |product|amount|price|
+------+-------+------+-----+
|store2|null   |20    |2000 |
|store2|null   |10    |5000 |
|store1|null   |15    |1000 |
|store1|pen    |20    |5000 |
+------+-------+------+-----+

scala> df.describe()
res46: org.apache.spark.sql.DataFrame = [summary: string, store: string ... 3 more fields]

scala> res46.collect()
res47: Array[org.apache.spark.sql.Row] = Array([count,4,1,4,4], [mean,null,null,16.25,3250.0], [stddev,null,null,4.7871355387816905,2061.5528128088304], [min,store1,pen,10,1000], [max,store2,pen,20,5000])
```

**윈도화**  
윈도 함수는 행들의 범위나 윈도로 하는 작업을 더 쉽게 할 수 있도록 한다. 윈도 함수는 노이즈 데이터를 포함하는 평균 속도 계산, 상대적인 매출 계산 등에 매우 유용하게 쓰인다.  
윈도 사양(WindowSpec)을 지정하고 나면 해당 윈도 위에서 함수를 지정해 연산을 할 수 있다.

```scala
import org.apache.spark.sql.expressions.Window

Window.partitionBy(df("store")).orderBy(df("amount"))
res0: org.apache.spark.sql.expressions.WindowSpec = org.apache.spark.sql.expressions.WindowSpec@2bba8eeb

scala> df.withColumn("rank", rank().over(res0)).filter(expr("rank <= 1")).select("*").show()
+------+-------+------+-----+----+
| store|product|amount|price|rank|
+------+-------+------+-----+----+
|store2|    bag|    10| 5000|   1|
|store1|   note|    15| 1000|   1|
+------+-------+------+-----+----+
```

**정렬**  
```scala
scala> df.orderBy(df("price").asc).show()
+------+-------+------+-----+
| store|product|amount|price|
+------+-------+------+-----+
|store1|   note|    15| 1000|
|store2|   note|    20| 2000|
|store2|    bag|    10| 5000|
|store1|    pen|    20| 5000|
+------+-------+------+-----+

scala> df.orderBy(df("price").asc).limit(2)
res4: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [store: string, product: string ... 2 more fields]

scala> res4.show()
+------+-------+------+-----+
| store|product|amount|price|
+------+-------+------+-----+
|store1|   note|    15| 1000|
|store2|   note|    20| 2000|
+------+-------+------+-----+
```

### 다중 DataFrame 트랜스포메이션  

**유사 집합 연산**  
DataFrame의 유사 집합 연산(set-like operation)은 가장 흔하게 집합 연산으로 떠올릴 만한 여러 작업들을 처리해준다. e.g) 합집합, 교집합, 차집합.
특히 unionAll의 경우는 갑들끼리의 비교가 필요 없기 때문에 비용이 가장 낮다.

### 전통적인 SQL 질의/하이브 데이터와 상호 연동하기
때때로 DataFrame에 연산식을 구축하는 것보다는 일반적인 SQL을 쓰는 것이 더 나을 때도 있다. 만약, 하이브 메타스토어에 연결이 되어 있다면 우리는 하이브 테이블을 대상으로 직접 SQL 질의를 작성해서 결과를 DataFrame으로 받을 수도 있다. 혹은 SQL 질의를 직접 작성하고 싶은 대상 DataFrame이 있다면 임시 테이블로 등록할 수도 있다.

```scala
scala> df.createOrReplaceTempView("test")

scala> spark.sql("select * from test").show()
+------+-------+------+-----+
| store|product|amount|price|
+------+-------+------+-----+
|store2|   note|    20| 2000|
|store2|    bag|    10| 5000|
|store1|   note|    15| 1000|
|store1|    pen|    20| 5000|
+------+-------+------+-----+
```

<br>

## DataFrame과 Dataset에서의 데이터 표현
DataFrame이란 단순히 Row객체를 모아놓은 RDD 이상의 것이다. DataFrame/Dataset은 특화된 데이터 표현 방식과 칼럼 기반 캐시 포맷을 갖고 있다. 특화된 표현 방식은 공간 효율성이 뛰어날 뿐 아니라 크리오(Kryo) 직렬화보다 더 빠르게 인코딩이 가능하다.  
RDD와 동일하게 DataFrame/Dataset은 일반적으로 lazy evaluation을 수행하며 종속성에 대한 계보를 구축한다. (DataFrame에서는 이를 논리적 계획(logical plan) 이라  부른다.)

### 텅스텐
텅스텐은 Spark SQL의 새로운 컴포넌트이며 바이트 단위 레벨에서 직접 동작하면서 더 효과적인 스파크 연산을 제공한다. 캐시된 RDD와 DataFrame 사이에서도 크기 차이를 확인할 수 있다. 텅스텐은 스파크에 필요한 연산 형태에 따라 최적화된 특수한 메모리 데이터 구조, 향상된 코드 생성, 특화된 프로토콜을 포함하고 있다.  

텅스텐의 데이터 표현 방식은 자바나 크리오 직렬화를 쓰는 것보다도 상당히 용량이 작다. 포맷만 더 콤팩트해진게 아니라 직렬화 소요 시간도 기본 직렬화보다 매우 빠르다.

텅스텐은 Spark 1.5에서 기본으로 탑재되었고, default = true로 되어 있다. 심지어 텅스텐이 아니더라도 Spark SQL은 크리오 직렬화와 칼럼 기반 저장 포맷을 써서 저장 비용을 최소화할 수 있다.

```scala
import spark.implicits._
import org.apache.spark.sql.functions._

val d1 = ("store2", "note", 20, 2000)
val d2 = ("store2", "bag", 10, 5000)
val d3 = ("store1", "note", 15, 1000)
val d4 = ("store1", "pen", 20, 5000)

val data = Seq(d1, d2, d3,d4)
val df = data.toDF("store", "product", "amount", "price")
val rdd = df.rdd

df.cache()
df.count() // for caching

rdd.cache()
rdd.count() // for caching
```

