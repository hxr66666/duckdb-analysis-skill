---
name: duckdb-data-analysis
description: |
  适用于查询、分析和可视化数据。例如分析csv、excel、parquet、json、jsonl  等文件的数据,进行统计聚合分类查询。先导入数据,然后使用SQL语句查询、分析和可视化数据。

  **支持的文件格式**:
  - CSV 文件: 支持直接加载，包括压缩的CSV文件（如.gz）
  - Excel 文件: 支持.xlsx和.xls格式
  - Parquet 文件: 支持列式存储格式
  - JSON 文件: 支持标准JSON格式
  - JSONL 文件: 支持每行一个JSON对象的格式

  **文件导入说明**:
  - 基于DuckDB官方推荐的导入方法
  - 自动处理文件格式识别和加载
  - 支持相对路径和绝对路径
  - 支持压缩文件的直接加载

  **WHEN TO USE:**
  - 你需要查询、分析和可视化数据。
  - 你需要对数据进行统计聚合分类查询。
  - 你需要处理和分析多种格式的数据文件。
  - 你需要生成数据可视化图表。

  **EXAMPLE USAGE:**
  - 分析销售数据，生成销售趋势图表
  - 处理CSV文件中的用户数据，进行用户行为分析
  - 分析Excel文件中的财务数据，生成财务报表
  - 处理JSON格式的日志数据，进行日志分析
---

## Quick Reference
| Task | Guide |
|------|-------|
| duckdb sql 语法指导规范，DuckDB的SQL方言紧密遵循PostgreSQL方言的约定，只有少数例外。 | 读取 [duckdb.md](duckdb.md) |

**注意事项**:
- 确保系统安装了uv工具,用于管理Python依赖
- 首次运行必须在`scripts` 目录下执行`uv sync` 命令,安装必要的依赖
- 首次运行必须先执行 `duckdb-analysis --session-id init  initialize-extensions` 命令下载duckdb 内置的数据插件，这个过程可能需要几分钟，视网络情况而定
- 首次运行时会创建 `analysis.db` 数据库文件,用于存储会话信息和操作日志

**参数说明**:
DuckDB 数据分析工具

positional arguments:
  {initialize-extensions,import,list-tables,table-metadata,query,visualize,log,describe,list-sessions,export-csv}
                        操作命令
    initialize-extensions 初始化扩展（第一次运行时下载和安装所有必要的扩展）
    import              导入数据
    list-tables         查看已导入的表
    table-metadata      获取表的元数据（字段信息）
    query               执行SQL查询
    visualize           数据可视化
    log                 查看操作日志
    describe            设置会话描述
    list-sessions       查看所有会话
    export-csv          导出查询结果为CSV文件

options:
  -h, --help            show this help message and exit
  --session-id SESSION_ID 会话ID，可以传入当前ai对话id用于关联当前ai对话与数据文件一对一绑定。也可以是个随机字符串

 **使用方法:**
  1. **查看帮助信息**:
     ```bash
     duckdb-analysis --help
     ```

  2. **初始化扩展** (首次运行时使用):
     ```bash
     duckdb-analysis --session-id <任意会话ID> initialize-extensions
     ```

  3. **导入数据**:
     ```bash
     duckdb-analysis --session-id <会话ID> import --file <文件路径> --table <表名> [--description <会话描述>]
     ```

  4. **查看已导入的表**:
     ```bash
     duckdb-analysis --session-id <会话ID> list-tables
     ```

  5. **获取表元数据**:
     ```bash
     duckdb-analysis --session-id <会话ID> table-metadata --table <表名>
     ```

  6. **执行SQL查询**:
     ```bash
     duckdb-analysis --session-id <会话ID> query --sql "<SQL语句>"
     ```

  7. **数据可视化**:
     - 柱状图:
       ```bash
       duckdb-analysis --session-id <会话ID> visualize --sql "<SQL语句>" --chart bar --x <x轴字段> --y <y轴字段>
       ```
     - 折线图:
       ```bash
       duckdb-analysis --session-id <会话ID> visualize --sql "<SQL语句>" --chart line --x <x轴字段> --y <y轴字段>
       ```
     - 散点图:
       ```bash
       duckdb-analysis --session-id <会话ID> visualize --sql "<SQL语句>" --chart scatter --x <x轴字段> --y <y轴字段>
       ```
     - 直方图:
       ```bash
       duckdb-analysis --session-id <会话ID> visualize --sql "<SQL语句>" --chart histogram --x <x轴字段>
       ```
     - 箱线图:
       ```bash
       duckdb-analysis --session-id <会话ID> visualize --sql "<SQL语句>" --chart box --x <x轴字段> --y <y轴字段>
       ```

  8. **查看操作日志**:
     ```bash
     duckdb-analysis --session-id <会话ID> log
     ```

  9. **设置会话描述**:
     ```bash
     duckdb-analysis --session-id <会话ID> describe --description "<会话描述>"
     ```

  10. **查看所有会话**:
     ```bash
     duckdb-analysis --session-id <任意会话ID> list-sessions
     ```

  11. **导出查询结果为CSV**:
     ```bash
     duckdb-analysis --session-id <会话ID> export-csv --sql "<SQL语句>" --output <输出文件路径>
     ```

  **示例操作**:
  1. **导入CSV文件**:
     ```bash
     duckdb-analysis --session-id sales_analysis import --file sales.csv --table sales_data --description "销售数据分析"
     ```

  2. **导入压缩CSV文件**:
     ```bash
     duckdb-analysis --session-id sales_analysis import --file sales.csv.gz --table sales_data
     ```

  3. **导入Excel文件**:
     ```bash
     duckdb-analysis --session-id sales_analysis import --file sales.xlsx --table sales_data
     ```

  4. **导入Parquet文件**:
     ```bash
     duckdb-analysis --session-id sales_analysis import --file sales.parquet --table sales_data
     ```

  5. **导入JSON文件**:
     ```bash
     duckdb-analysis --session-id sales_analysis import --file sales.json --table sales_data
     ```

  6. **导入JSONL文件**:
     ```bash
     duckdb-analysis --session-id sales_analysis import --file sales.jsonl --table sales_data
     ```

  7. **使用相对路径导入**:
     ```bash
     duckdb-analysis --session-id sales_analysis import --file data/sales.csv --table sales_data
     ```

  8. **使用绝对路径导入**:
     ```bash
     duckdb-analysis --session-id sales_analysis import --file C:/data/sales.csv --table sales_data
     ```

  9. **获取表元数据**:
     ```bash
     duckdb-analysis --session-id sales_analysis table-metadata --table sales_data
     ```

  10. **基本查询**:
     ```bash
     duckdb-analysis --session-id sales_analysis query --sql "SELECT * FROM sales_data LIMIT 10"
     ```

  11. **聚合查询**:
     ```bash
     duckdb-analysis --session-id sales_analysis query --sql "SELECT region, SUM(amount) as total FROM sales_data GROUP BY region"
     ```

  12. **分组统计**:
     ```bash
     duckdb-analysis --session-id sales_analysis query --sql "SELECT region, product, COUNT(*) as count, AVG(amount) as avg_amount FROM sales_data GROUP BY region, product"
     ```

  13. **条件查询**:
     ```bash
     duckdb-analysis --session-id sales_analysis query --sql "SELECT * FROM sales_data WHERE amount > 1000 AND region = 'North'"
     ```

  14. **排序查询**:
     ```bash
     duckdb-analysis --session-id sales_analysis query --sql "SELECT region, product, amount FROM sales_data ORDER BY amount DESC LIMIT 10"
     ```

  15. **日期分析**:
     ```bash
     duckdb-analysis --session-id sales_analysis query --sql "SELECT DATE_TRUNC('month', sale_date) as month, SUM(amount) as total FROM sales_data GROUP BY month ORDER BY month"
     ```

  16. **连接查询**:
     ```bash
     duckdb-analysis --session-id sales_analysis query --sql "SELECT s.region, s.product, s.amount, p.category FROM sales_data s JOIN products p ON s.product_id = p.id"
     ```

  17. **子查询**:
     ```bash
     duckdb-analysis --session-id sales_analysis query --sql "SELECT region, AVG(amount) as avg_amount FROM sales_data GROUP BY region HAVING AVG(amount) > (SELECT AVG(amount) FROM sales_data)"
     ```

  18. **柱状图可视化**:
     ```bash
     duckdb-analysis --session-id sales_analysis visualize --sql "SELECT region, SUM(amount) as total FROM sales_data GROUP BY region" --chart bar --x region --y total
     ```

  19. **折线图可视化**:
     ```bash
     duckdb-analysis --session-id sales_analysis visualize --sql "SELECT DATE_TRUNC('month', sale_date) as month, SUM(amount) as total FROM sales_data GROUP BY month ORDER BY month" --chart line --x month --y total
     ```

  20. **散点图可视化**:
     ```bash
     duckdb-analysis --session-id sales_analysis visualize --sql "SELECT price, quantity FROM sales_data" --chart scatter --x price --y quantity
     ```

  21. **直方图可视化**:
     ```bash
     duckdb-analysis --session-id sales_analysis visualize --sql "SELECT amount FROM sales_data" --chart histogram --x amount
     ```

  22. **箱线图可视化**:
     ```bash
     duckdb-analysis --session-id sales_analysis visualize --sql "SELECT region, amount FROM sales_data" --chart box --x region --y amount
     ```

  23. **设置会话描述**:
     ```bash
     duckdb-analysis --session-id sales_analysis describe --description "2024年销售数据分析"
     ```

  24. **查看操作日志**:
     ```bash
     duckdb-analysis --session-id sales_analysis log
     ```

  25. **查看所有会话**:
     ```bash
     duckdb-analysis --session-id test list-sessions
     ```

  26. **导出查询结果为CSV**:
     ```bash
     duckdb-analysis --session-id sales_analysis export-csv --sql "SELECT region, SUM(amount) as total FROM sales_data GROUP BY region" --output sales_summary.csv
     ```