
# 数据库的group by实现探究

## 一、探索目标

### 1、研究背景

在关系数据库中，通常可以使用group by语句进行分组聚合计算，探究其实现原理，基于csv文件作为数据的输入，采用java语言编写实现一个简易的group by算法,并支持千万级数据的计算。

### 2、问题描述

程序从CSV 格式的文件中读取数据(例如foo.csv)，每行包含英文逗号分隔的两个整数，其中含有千万行模拟数据，可以看做个 foo(a int, b int) 的数据库关系表，样例如下: 

```
a,b
2,5
1,2
2,2
4,6
2,4
5,3
3,2
4,4
3,4
1,6
1,5
```

实现:

`SELECT AVG(a), b from foo group by b;`

并将结果简单的直接在标准输出中。 

### 3、要求

- 不要使用任何第三方软件如(数据库，Kafka等)，csv解析库等通常的工具库除外；

- 查询执行的速度越快越好，尽可能的使内存，CPU的资源(这里假设内存有能力完全装载数据文件) ；

- 完成测试，包括单元测试和回归测试 ；

- 使用Markdown格式的README介绍使用的算法以及整体思路； 

- 完成程序的性能分析，指出性能瓶颈以及未来优化方向，以及有没有其他方式完成这道题。并指出适用的场景；

## 二、模拟数据制作

这里基于MySQL的csv存储引擎和RAND()函数来简化制作模拟数据,SQL脚本如下：

### 1、创建一个csv存储引擎的表

```
-- 创建一个csv存储引擎的模拟测试数据表
CREATE TABLE `t_foo` (
  `a` bigint(255) NOT NULL,
  `b` bigint(255) NOT NULL
) ENGINE=CSV DEFAULT CHARSET=utf8mb4 COMMENT='两千万行模拟数据测试';
```

### 2、向表中插入两千万条模拟数据

```
-- 使用随机数向表内生产出两千万的模拟数据
DROP PROCEDURE IF EXISTS PROC_INIT_DATA_t_foo;
DELIMITER $
CREATE PROCEDURE PROC_INIT_DATA_t_foo()
BEGIN
    DECLARE i INT DEFAULT 0;
    while i<=20000000 do
			insert into `t_foo`(`a`,`b`) values( FLOOR(1 + (RAND() * 5)), FLOOR(2 + (RAND() * 5)) );
			SET i = i+1;
 END WHILE;
END $
CALL PROC_INIT_DATA_t_foo();
```

### 3、验证和查询模拟的数据总量

```
-- 查询模拟的数据总量
SELECT count(*) from `t_foo`;

-- 查询计算结果
SELECT avg(a) as `avg_a`,b FROM `t_foo` GROUP BY b;
```

### 4、基于MySQL制造模拟数据的好处

通过MySQL的csv引擎制作的模拟数据，一方面可以直接从MySQL的数据目录里获取到csv格式的数据文件，另一方面，可以直接用MySQL的group by的SQL语句获取计算结果，方便直接与下文程序计算的结果进行比较，**验证程序结果的正确性**。

## 三、实现思路

### 1、思路入口点

先分析下MySQL的group by语句查询出来的结果集合的特点：

```
mysql> SELECT * from `foo`;           
+---+---+
| a | b |
+---+---+
| 2 | 5 |
| 1 | 2 |
| 2 | 2 |
| 4 | 6 |
| 2 | 4 |
| 5 | 3 |
| 3 | 2 |
| 4 | 4 |
| 3 | 4 |
| 1 | 6 |
| 1 | 5 |
+---+---+
11 rows in set

mysql> SELECT * from `foo` ORDER BY b;
+---+---+
| a | b |
+---+---+
| 1 | 2 |
| 2 | 2 |
| 3 | 2 |
| 5 | 3 |
| 3 | 4 |
| 2 | 4 |
| 4 | 4 |
| 2 | 5 |
| 1 | 5 |
| 4 | 6 |
| 1 | 6 |
+---+---+
11 rows in set

mysql> SELECT avg(a) as `avg_a`,b FROM `foo` GROUP BY b;
+--------+---+
| avg_a  | b |
+--------+---+
| 2.0000 | 2 |
| 5.0000 | 3 |
| 3.0000 | 4 |
| 1.5000 | 5 |
| 2.5000 | 6 |
+--------+---+
5 rows in set
```

**分析**：
	
> 由于SQL语句按照b字段进行group by，结果集中b的值是递增有序的，而原始表的结果集合中b的并非有序，但经过对b字段进行order by后，结果的顺序接近于group by的结果集合的顺序。

**结论**:

> group by操作需要先对表内所有数据内容按照group by后的字段进行递增排序;

### 2、思路实现

步骤一：将csv文件内容全部加载到内存中,即得到表：

<table border="1">
	<tr>
	    <th>a</th>
	    <th>b</th>
	</tr>
	<tr>
    <td>2</td>
	  <td>5</td>
	</tr>
  <tr>
    <td>1</td>
	  <td>2</td>
	</tr>
  <tr>
    <td>2</td>
	  <td>2</td>
	</tr>
  <tr>
    <td>4</td>
	  <td>6</td>
	</tr>
  <tr>
    <td>2</td>
	  <td>4</td>
	</tr>
  <tr>
    <td>5</td>
	  <td>3</td>
	</tr>
  <tr>
    <td>3</td>
	  <td>2</td>
	</tr>
  <tr>
    <td>4</td>
	  <td>4</td>
	</tr>
  <tr>
    <td>3</td>
	  <td>4</td>
	</tr>
  <tr>
    <td>1</td>
	  <td>6</td>
	</tr>
  <tr>
    <td>1</td>
	  <td>5</td>
	</tr>
</table>

步骤二：按照group by后的字段进行递增排序

<table border="1">
	<tr>
	    <th>a</th>
	    <th>b</th>
	</tr>
	<tr>
    <td>1</td>
	  <td>2</td>
	</tr>
  <tr>
    <td>2</td>
	  <td>2</td>
	</tr>
  <tr>
    <td>3</td>
	  <td>2</td>
	</tr>
  <tr>
    <td>5</td>
	  <td>3</td>
	</tr>
  <tr>
    <td>3</td>
	  <td>4</td>
	</tr>
  <tr>
    <td>2</td>
	  <td>4</td>
	</tr>
  <tr>
    <td>4</td>
	  <td>4</td>
	</tr>
  <tr>
    <td>2</td>
	  <td>5</td>
	</tr>
  <tr>
    <td>1</td>
	  <td>5</td>
	</tr>
  <tr>
    <td>4</td>
	  <td>6</td>
	</tr>
  <tr>
    <td>1</td>
	  <td>6</td>
	</tr>
</table>

步骤三：使用聚合函数归并计算

- 归并过程

<table border="1">
	<tr>
	    <th>a</th>
	    <th>b</th>
	</tr >
	<tr >
    <td>1</td>
	  <td rowspan="3">2</td>
	</tr>
	<tr>
	    <td>2</td>
	</tr>
	<tr>
	    <td>3</td>
	</tr>
	<tr>
    <td>5</td>
	  <td>3</td>
	</tr>
	<tr>
	    <td>3</td>
	    <td rowspan="3">4</td>
	</tr>
	<tr>
	    <td>2</td>
	</tr>
	<tr>
	    <td>4</td>
	</tr>
	<tr>
	    <td>2</td>
	    <td rowspan="2">5</td>
	</tr>
	<tr>
	    <td>1</td>
	</tr>
	<tr>
	    <td>4</td>
	    <td rowspan="2">6</td>
	</tr>
	<tr>
	    <td>1</td>
	</tr>
</table>

- 聚合计算

<table border="1">
	<tr>
	    <th>avg(a)</th>
	    <th>b</th>
	</tr >
	<tr >
    <td>2.0000</td>
	  <td>2</td>
	</tr>
	<tr>
	    <td>5.0000</td>
	    <td>3</td>
	</tr>
	<tr>
	    <td>3.0000</td>
	    <td>4</td>
	</tr>
	<tr>
	    <td>1.5000</td>
	    <td>5</td>
	</tr>
	<tr>
	    <td>2.5000</td>
	    <td>6</td>
	</tr>
</table>

## 三、基于java的设计实现

### 1、核心思路

加载数据到内存 --> 字段排序 --> 归并聚合 -->输出结果集

- (1) 加载数据到内存

利用javacsv库将csv文本文件内容全部加载到内存中：

```
URL resource = ExampleTest.class.getClassLoader().getResource("test.csv");
DataRecordSet data=CsvFileUtils.readCsvFile(resource.getFile());
```

说明：CSV文本文件在内存中的存储格式为: 单行记录用String[]存储，多行记录用户ArrayList<String[]>存储。

- (2) 字段排序

利用jdk里的Collections.sort()里的TimSort排序算法：

```
		Collections.sort(dataRecordSet.getContent(), new Comparator<String[]>() {

			@Override
			public int compare(String[] left, String[] rigth) {
				for (Map.Entry<String, StringToNumberConverter<?>> entry : groupByFieldsMap.entrySet()) {
					String field = entry.getKey();
					StringToNumberConverter<?> converter = entry.getValue();
					
					int index = dataRecordSet.getFieldIndex(field);
					
					Number o1 = converter.convert(left[index]);
					Number o2 = converter.convert(rigth[index]);

					// Here o1 and o2 is all implement Comparable
					@SuppressWarnings("rawtypes")
					Comparable c1 = (Comparable) o1;
					@SuppressWarnings("rawtypes")
					Comparable c2 = (Comparable) o2;

					@SuppressWarnings("unchecked")
					int ret = c1.compareTo(c2);
					if (0 != ret) {
						return ret;
					}
				}

				return 0;
			}

		});
```

- (2) 归并聚合

归并聚合完全自己实现，实现的核心代码如下(见com.github.tang.groupby.core.DefaultGroupByService.aggregateData(Aggregation<?, ?>...))：

```
	private List<Map<String, String>> aggregateData(Aggregation<?, ?>... aggregations) {
		/* 存储计算结果的结果集 */
		List<Map<String, String>> result = new LinkedList<>();
		/* 获取group的所有字段 */
		Set<String> groupFields = groupByFieldsMap.keySet();
		/* 获取非group的所有字段 */
		Set<String> otherFields = queryFieldsMap.keySet().stream().filter((i) -> !groupFields.contains(i)).collect(Collectors.toSet());

		/* 记录聚合计算的起止数据游标 */
		int first = 0, last = 0;

		/**
		 * <p>
		 * 1、 遍历数据集
		 * </p>
		 * <p>
		 * 2、 归并字段
		 * </p>
		 * <p>
		 * 3、 聚合计算
		 * </p>
		 */
		int i;
		for (i = 0; i < dataRecordSet.getContent().size(); ++i) {
			String[] originRow = dataRecordSet.getContent().get(i);

			if (result.isEmpty()) {
				// 当首次结果集为空时，先将分组字段数据存储，整行记录不完整
				Map<String, String> targetRow = new HashMap<>();
				for (String gf : groupFields) {
					targetRow.put(gf, originRow[dataRecordSet.getFieldIndex(gf)]);
				}

				result.add(targetRow);
				continue;
			}

			if (canMergeToLast(originRow, result, groupFields)) {
				// 遇到可以聚合的行时，只需更新last游标即可
				last = i;
			} else {
				// 找到一个可以进行聚合计算的结果
				if (i > 0 && first <= last) {
					Map<String, String> targetRow = result.get(result.size() - 1);

					// 调用聚合函数进行计算,聚合函数由用户从外部传入
					for (String of : otherFields) {
						Aggregation<?, ?> aggregation = queryFieldsMap.get(of);
						List<String[]> subData = dataRecordSet.getSubContent(first, last + 1);
						Number v = aggregation.aggregation(dataRecordSet.getHeader(), subData);
						targetRow.put(aggregation.getAggregationFunctionName() + "_" + of, v.toString());
					}

					targetRow = new HashMap<>();
					for (String f : groupFields) {
						targetRow.put(f, originRow[dataRecordSet.getFieldIndex(f)]);
					}

					result.add(targetRow);
				}

				first = last + 1;
				last = first;
			}

		}

		/**
		 * 调用聚合函数补充最后一行缺失的字段值
		 */
		Map<String, String> targetRow = result.get(result.size() - 1);

		// 调用聚合函数进行计算
		for (String of : otherFields) {
			Aggregation<?, ?> aggregation = queryFieldsMap.get(of);
			List<String[]> subData = dataRecordSet.getSubContent(first, last + 1);
			Number v = aggregation.aggregation(dataRecordSet.getHeader(), subData);
			targetRow.put(aggregation.getAggregationFunctionName() + "_" + of, v.toString());
		}

		return result;
	}
```

### 2、环境与编译

- 环境说明

  **JDK**:>=1.8
 
  **maven**:>=3.1 (建议配置阿里云仓库)

- 全部编译测试

```
# git clone https://github.com/tangyibo/java-group-by.git
# cd java-group-by/
# mvn clean
# mvn test
```

- 分模块编译测试

**(1)本文算法测试**

```
# git clone https://github.com/tangyibo/java-group-by.git
# cd java-group-by/java-group-by-tang/
# mvn clean
# mvn test
```

**(2)calcite算法测试**

```
# git clone https://github.com/tangyibo/java-group-by.git
# cd java-group-by/java-group-by-calcite/
# mvn clean
# mvn test
```

**(3)mysql算法测试**

请参照第二章节的方式。


## 四、性能测试与分析

### 1、已有的实现方式

- 基于calcite的计算

Apache Calcite 是一款开源SQL解析工具, 可以将各种SQL语句解析成抽象语法术AST(Abstract Syntax Tree), 之后通过操作AST就可以把SQL中所要表达的算法与关系体现在具体代码之中。

Calcite提供了多种方式添加数据源,其中包括csv文本文件作为输入数据源，并提供jdbc方式执行SQL进行计算的功能。示例代码：

```
		Properties config = new Properties();
		config.put("model", CalciteTest.class.getClassLoader().getResource("config.json").getPath());
		config.put("caseSensitive", "false");
		
		Connection connection = DriverManager.getConnection("jdbc:calcite:", config);
		CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

		String sql = "select avg(a) as \"avg_a\",b from csv.test group by b";
		Statement statement = calciteConnection.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		ResultSetMetaData rsmd=rs.getMetaData();
		
		while(rs.next()) {
			Map<String,Object> row=new HashMap<>();
			for(int i=1;i<=rsmd.getColumnCount();++i) {
				row.put(rsmd.getColumnName(i), rs.getObject(i));
			}
			
			System.out.println(row);
		}
		
		rs.close();
		statement.close();
		connection.close();
```

- 基于MySQl的csv引擎的计算

MySQL的csv存储引擎支持csv格式的文本方式存储数据，并可通过SQL语句的方式直接进行查询计算。

### 2、性能测试对比

本文的算法(下文简称myself)与calcite和mysql进行了性能对比测试，分别以100w、500w、800w、1000w、2000w的数据量进行对比测试，结果如下：

#### （1）工作机测试

| 数据量 | myself |  calcite  |  MySQL  |
| :--- |  :---: |  :---: |  :---: |
| 100w | 0.515 |  0.323  |  1.155  |
| 500w | 3.375 |  1.209  |  4.428  |
| 600w | 5.262 |  1.299   |  4.618  |
| 700w | 7.427 |  1.487   |  5.692  |
| 800w | 7.188  |  1.679  |  6.014  |
| 1000w | 23.390 |  2.726  |  7.042  |
| 2000w | OOM |  4.510  |  13.639  |

注：

- (1) 测试环境：自己的工作机器，OS: Win10 Home, Mem: 8G , CPU : 1个CPU/4核(Intel i3)；

- (2) JVM参数默认配置；

- (3) 性能时间单位为秒；

- (4) OOM的类型为：java.lang.OutOfMemoryError: GC overhead limit exceeded，发生在csv加载到内存的阶段，堆栈信息如下：

```
java.lang.OutOfMemoryError: GC overhead limit exceeded
	at java.util.Arrays.copyOfRange(Unknown Source)
	at java.lang.String.<init>(Unknown Source)
	at com.csvreader.CsvReader.endColumn(Unknown Source)
	at com.csvreader.CsvReader.readRecord(Unknown Source)
	at com.github.tang.groupby.util.CsvFileUtils.readCsvFile(CsvFileUtils.java:44)
	at com.github.tang.groupby.PerformanceTest.testGroupByServiceWithFooAvg2000w(PerformanceTest.java:139)
```

#### （2）服务器测试

| 数据量 | myself |  calcite  |  MySQL  |
| :--- |  :---: |  :---: |  :---: |
| 100w | 0.933 |  0.411  |  1.155  |
| 500w | 6.929 |  1.802  |  4.428  |
| 600w | 7.295 |  2.131   |  4.618  |
| 700w | 7.458 |  2.487   |  5.692  |
| 800w | 7.852  |  2.819  |  6.014  |
| 1000w | 23.657 |  3.755  |  7.042  |
| 2000w | 43.677 |  6.745  |  13.639  |

注：

- (1) 测试环境：CentOS服务器，OS: CentOS Linux release 7.5.1804 (Core), Mem: 19G , CPU : 8个CPU/8核(Intel(R) Xeon(R) Silver 4208 CPU @ 2.10GHz)；

- (2) JVM参数默认配置；

- (3) 性能时间单位为秒；

- (4) 未发生OOM；

### 3、性能问题分析

- (1) OOM问题分析：myself的数据在加载至内存的过程中就发生了OOM

- (2) 速度问题分析： 从执行速度上来看，calcite > MySQL > myself

目前尚未细读过calcite与mysql的group by的实现代码，算法复杂度的比较分析暂时无法给出。

- (3) 改进优化点尝试

> 支持group by多个字段的情况；

> 扩大支持的数据类型的范围；

- (4) 引入新的思路

> 研究calcite中group by的实现方法；

## 五、总结

经过此次探究，较为深入的了解了group by底层的大致原理，并基本算是实现了一个简单版的avg(),min(),max(),sum(),count()等聚合函数的group by功能实现。但目前支持含有一个整形字段的group by，对于多字段和其他数据类型的支持含有很大差距。

从测试结果来看，目前**能支千万级别左右的数据量(依赖物理内存)**，而支持上亿级的数据量目标还需要继续努力!!!