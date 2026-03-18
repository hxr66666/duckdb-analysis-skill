## DuckDB SQL语法说明

DuckDB支持标准SQL语法，并添加了许多扩展功能。以下是DuckDB SQL语法的主要组成部分：

### 1. Introduction
DuckDB是一个关系型数据库管理系统（RDBMS），支持标准SQL语法，并添加了许多针对分析场景的扩展功能。它设计为轻量级、高性能，特别适合处理和分析大型数据集。

DuckDB的SQL方言紧密遵循PostgreSQL方言的约定，只有少数例外。它是一个嵌入式数据库，意味着它可以直接嵌入到应用程序中，不需要单独的服务器进程。

### 2. Statements
DuckDB支持丰富的SQL语句，包括：
- `SELECT` - 从表中检索数据
- `INSERT` - 向表中插入数据
- `UPDATE` - 更新表中的数据
- `DELETE` - 从表中删除数据
- `CREATE TABLE` - 创建新表
- `ALTER TABLE` - 修改现有表
- `DROP TABLE` - 删除表
- `CREATE VIEW` - 创建视图
- `CREATE INDEX` - 创建索引
- `CREATE SCHEMA` - 创建模式
- `CREATE MACRO` - 创建宏
- `CREATE SEQUENCE` - 创建序列
- `CREATE TYPE` - 创建类型
- `COPY` - 批量导入/导出数据
- `MERGE INTO` - 合并数据
- `EXPLAIN` - 查看查询执行计划
- `ANALYZE` - 分析表统计信息
- `VACUUM` - 清理数据库
- `CHECKPOINT` - 创建检查点
- `CALL` - 调用存储过程
- `SET` / `RESET` - 设置/重置配置
- `SHOW` - 显示系统信息
- `TRANSACTION` - 事务管理

### 3. Query Syntax
DuckDB的查询语法遵循标准SQL，支持：
- `SELECT` 子句 - 指定要检索的列
- `FROM` 子句 - 指定数据来源
- `WHERE` 子句 - 过滤数据
- `GROUP BY` 子句 - 分组数据
- `HAVING` 子句 - 过滤分组结果
- `ORDER BY` 子句 - 排序结果
- `LIMIT` 子句 - 限制结果数量
- `OFFSET` 子句 - 跳过结果数量
- `JOIN` 子句 - 连接多个表
- `WITH` 子句 - 定义公共表表达式（CTE）
- `PIVOT` / `UNPIVOT` - 数据透视和逆透视

### 4. Data Types
DuckDB支持丰富的数据类型，包括：

#### 通用数据类型
- **数值类型**：`TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `HUGEINT`, `FLOAT`, `DOUBLE`, `DECIMAL`
- **字符串类型**：`VARCHAR`, `CHAR`, `TEXT`
- **日期时间类型**：`DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMP WITH TIME ZONE`, `INTERVAL`
- **布尔类型**：`BOOLEAN`
- **其他类型**：`UUID`, `JSON`, `BLOB`

#### 复合数据类型
- `ARRAY` - 固定长度的同类型数据序列
- `LIST` - 可变长度的同类型数据序列
- `MAP` - 键值对字典
- `STRUCT` - 命名字段的结构体
- `UNION` - 多种类型的联合

### 5. Expressions
DuckDB支持各种表达式，包括：
- **算术表达式**：`+`, `-`, `*`, `/`, `%`
- **比较表达式**：`=`, `<`, `>`, `<=`, `>=`, `<>`
- **逻辑表达式**：`AND`, `OR`, `NOT`
- **CASE表达式**：条件分支表达式
- **CAST表达式**：类型转换
- **IN操作符**：检查值是否在集合中
- **函数表达式**：内置函数和用户定义函数
- **聚合表达式**：`SUM`, `AVG`, `COUNT`, `MAX`, `MIN`
- **窗口函数表达式**：带窗口的函数调用
- **子查询**：嵌套的查询
- **TRY表达式**：捕获表达式执行错误

### 6. Functions
DuckDB提供了丰富的内置函数，包括：
- **数学函数**：`ABS`, `SQRT`, `POW`, `LOG`, `SIN`, `COS`
- **字符串函数**：`CONCAT`, `SUBSTRING`, `LOWER`, `UPPER`, `LENGTH`
- **日期时间函数**：`CURRENT_DATE`, `CURRENT_TIMESTAMP`, `DATE_TRUNC`, `EXTRACT`
- **聚合函数**：`SUM`, `AVG`, `COUNT`, `MAX`, `MIN`, `MEDIAN`
- **窗口函数**：`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LEAD`, `LAG`
- **JSON函数**：`JSON_EXTRACT`, `JSON_ARRAY`, `JSON_OBJECT`
- **数组函数**：`ARRAY_AGG`, `ARRAY_LENGTH`, `ARRAY_SLICE`
- **地图函数**：`MAP_AGG`, `MAP_KEYS`, `MAP_VALUES`
- **结构体函数**：`STRUCT_PACK`, `STRUCT_EXTRACT`
- **类型转换函数**：`CAST`, `TRY_CAST`
- **条件函数**：`CASE`, `COALESCE`, `NULLIF`

### 7. Constraints
DuckDB支持表级约束，包括：
- `PRIMARY KEY` - 主键约束
- `UNIQUE` - 唯一约束
- `NOT NULL` - 非空约束
- `CHECK` - 检查约束
- `FOREIGN KEY` - 外键约束

### 8. Indexes
DuckDB支持创建索引以提高查询性能：
- `CREATE INDEX` - 创建索引
- `DROP INDEX` - 删除索引
- 支持B-tree索引
- 支持哈希索引
- 支持部分索引
- 支持表达式索引

### 9. Meta Queries
DuckDB提供了丰富的元数据查询功能，包括：
- `PRAGMA table_info(table_name)` - 获取表的字段信息
- `SHOW TABLES` - 列出所有表
- `SHOW CREATE TABLE` - 显示表的创建语句
- `EXPLAIN` - 查看查询执行计划
- `INFORMATION_SCHEMA` - 标准信息模式
- `DUCKDB_TABLE_FUNCTIONS` - DuckDB特定的表函数
- `PRAGMA database_list` - 列出所有数据库
- `PRAGMA schema_list` - 列出所有模式

### 10. DuckDB's SQL Dialect
DuckDB的SQL方言有一些特点：
- 支持PostgreSQL风格的语法
- 支持扩展功能，如数组操作、JSON支持
- 支持直接从文件中读取数据（如CSV、JSON、Parquet）
- 支持CTE（公共表表达式）
- 支持窗口函数
- 支持复杂类型和操作
- 支持批量导入/导出数据
- 支持事务
- 支持子查询
- 支持视图

### 11. Samples
以下是一些DuckDB SQL示例：

**基本查询**
```sql
SELECT * FROM sales_data LIMIT 10;
```

**聚合查询**
```sql
SELECT region, SUM(amount) as total FROM sales_data GROUP BY region;
```

**窗口函数**
```sql
SELECT region, product, amount, 
       ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) as rank
FROM sales_data;
```

**日期分析**
```sql
SELECT DATE_TRUNC('month', sale_date) as month, 
       SUM(amount) as total 
FROM sales_data 
GROUP BY month 
ORDER BY month;
```

**JSON操作**
```sql
SELECT JSON_EXTRACT(data, '$.name') as name 
FROM json_data;
```

**直接从文件读取**
```sql
SELECT * FROM read_csv('sales.csv');
```

**创建表**
```sql
CREATE TABLE weather (
    city    VARCHAR,
    temp_lo INTEGER,
    temp_hi INTEGER,
    prcp    FLOAT,
    date    DATE
);
```

**插入数据**
```sql
INSERT INTO weather 
VALUES ('San Francisco', 46, 50, 0.25, '1994-11-27');
```

**更新数据**
```sql
UPDATE weather 
SET temp_lo = 45 
WHERE city = 'San Francisco' AND date = '1994-11-27';
```

**删除数据**
```sql
DELETE FROM weather 
WHERE date < '1994-01-01';
```

**连接查询**
```sql
SELECT w.city, w.temp_lo, w.temp_hi, c.lat, c.lon 
FROM weather w 
JOIN cities c ON w.city = c.name;
```

**CTE查询**
```sql
WITH regional_sales AS (
    SELECT region, SUM(amount) as total 
    FROM sales_data 
    GROUP BY region
)
SELECT region, total 
FROM regional_sales 
WHERE total > 100000;
```

### 参考资料
- [DuckDB官方文档](https://duckdb.org/docs/)
- [DuckDB SQL参考](https://duckdb.org/docs/sql/introduction)
- [DuckDB语句概述](https://duckdb.org/docs/stable/sql/statements/overview)
- [DuckDB数据类型](https://duckdb.org/docs/stable/sql/data_types/overview)
- [DuckDB表达式](https://duckdb.org/docs/stable/sql/expressions/overview)
- [DuckDB函数](https://duckdb.org/docs/stable/sql/functions/overview)
- [DuckDB约束](https://duckdb.org/docs/stable/sql/constraints)
- [DuckDB索引](https://duckdb.org/docs/stable/sql/indexes)
- [DuckDB元查询](https://duckdb.org/docs/stable/sql/meta/information_schema)
- [DuckDB表函数](https://duckdb.org/docs/stable/sql/meta/duckdb_table_functions)
- [DuckDB示例](https://duckdb.org/docs/stable/sql/samples)
