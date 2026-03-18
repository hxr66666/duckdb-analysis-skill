# DuckDB 数据分析工具

本仓库是一个给AI调用的skill仓库，基于DuckDB的轻量级数据分析工具，用于导入、查询、分析和可视化CSV、Excel、Parquet、JSON、JSONL等格式的数据。

## DuckDB 的优点：
- **轻量级**：无需安装复杂的数据库服务器，仅需一个可执行文件即可运行
- **快速**：基于列arstore 存储引擎，读写速度快
- **内存占用低**：所有数据都存储在内存中，无需磁盘IO
- **跨平台**：支持Windows、macOS和Linux等多个操作系统
- **SQL支持**：完整实现SQL标准，支持标准SQL语法
- **扩展丰富**：内置Excel、HTTP、JSON、Parquet等扩展，支持多种数据格式
- **社区活跃**：有一个活跃的社区，不断更新和完善功能

## 功能特点

- **多格式数据支持**：支持CSV、Excel、Parquet、JSON、JSONL等多种数据格式
- **会话管理**：每次AI会话生成独立的数据库文件，元数据库统一管理会话信息
- **操作日志**：所有操作记录到元数据库，支持查看历史操作
- **表元数据查询**：获取表的字段信息，便于AI了解表结构
- **SQL查询**：支持标准SQL语法，返回markdown格式的表格结果，方便AI处理和展示
- **数据可视化**：支持柱状图、折线图、散点图、直方图、箱线图等多种图表类型，结果保存为图片文件
- **CSV导出**：支持将查询结果导出为CSV文件，便于后续处理
- **扩展支持**：内置Excel、HTTP、JSON、Parquet等扩展，增强数据处理能力
- **AI友好**：命令行接口设计简洁明了，便于AI通过工具调用执行数据分析任务

## 安装步骤

1. **安装uv工具**（如果尚未安装）
   - 参考 [uv官方文档](https://docs.astral.sh/uv/) 安装uv

2. **克隆项目**
   ```bash
   git clone <项目地址>
   cd duckdb-analysis
   ```

3. **安装依赖**
   ```bash
   cd scripts
   uv sync
   ```

4. **初始化扩展**（首次运行时）
   ```bash
   cd ../
   uv run --directory scripts main.py --session-id init initialize-extensions
   ```
   这个过程可能需要几分钟，因为需要从网络下载扩展

## 使用方法

### 1. 查看帮助信息
```bash
uv run --directory scripts main.py --help
```

### 2. 导入数据
```bash
uv run --directory scripts main.py --session-id <会话ID> import --file <文件路径> --table <表名> [--description <会话描述>]
```

### 3. 查看已导入的表
```bash
uv run --directory scripts main.py --session-id <会话ID> list-tables
```

### 4. 获取表元数据
```bash
uv run --directory scripts main.py --session-id <会话ID> table-metadata --table <表名>
```

### 5. 执行SQL查询
```bash
uv run --directory scripts main.py --session-id <会话ID> query --sql "<SQL语句>"
```

### 6. 数据可视化
- 柱状图：
  ```bash
  uv run --directory scripts main.py --session-id <会话ID> visualize --sql "<SQL语句>" --chart bar --x <x轴字段> --y <y轴字段>
  ```
- 折线图：
  ```bash
  uv run --directory scripts main.py --session-id <会话ID> visualize --sql "<SQL语句>" --chart line --x <x轴字段> --y <y轴字段>
  ```
- 散点图：
  ```bash
  uv run --directory scripts main.py --session-id <会话ID> visualize --sql "<SQL语句>" --chart scatter --x <x轴字段> --y <y轴字段>
  ```
- 直方图：
  ```bash
  uv run --directory scripts main.py --session-id <会话ID> visualize --sql "<SQL语句>" --chart histogram --x <x轴字段>
  ```
- 箱线图：
  ```bash
  uv run --directory scripts main.py --session-id <会话ID> visualize --sql "<SQL语句>" --chart box --x <x轴字段> --y <y轴字段>
  ```

### 7. 查看操作日志
```bash
uv run --directory scripts main.py --session-id <会话ID> log
```

### 8. 设置会话描述
```bash
uv run --directory scripts main.py --session-id <会话ID> describe --description "<会话描述>"
```

### 9. 查看所有会话
```bash
uv run --directory scripts main.py --session-id <任意会话ID> list-sessions
```

### 10. 导出查询结果为CSV
```bash
uv run --directory scripts main.py --session-id <会话ID> export-csv --sql "<SQL语句>" --output <输出文件路径>
```

## 示例操作

### 导入CSV文件
```bash
uv run --directory scripts main.py --session-id sales_analysis import --file sales.csv --table sales_data --description "销售数据分析"
```

### 基本查询
```bash
uv run --directory scripts main.py --session-id sales_analysis query --sql "SELECT * FROM sales_data LIMIT 10"
```

### 聚合查询
```bash
uv run --directory scripts main.py --session-id sales_analysis query --sql "SELECT region, SUM(amount) as total FROM sales_data GROUP BY region"
```

### 数据可视化
```bash
uv run --directory scripts main.py --session-id sales_analysis visualize --sql "SELECT region, SUM(amount) as total FROM sales_data GROUP BY region" --chart bar --x region --y total
```

### 导出CSV
```bash
uv run --directory scripts main.py --session-id sales_analysis export-csv --sql "SELECT region, SUM(amount) as total FROM sales_data GROUP BY region" --output sales_summary.csv
```

## 项目结构

```
duckdb-analysis/
├── data/                # 数据存储目录
│   ├── analysis.db      # 元数据库
│   └── session_*.db     # 会话数据库文件
├── scripts/             # 脚本目录
│   ├── main.py          # 主脚本
│   └── pyproject.toml   # 依赖配置
├── SKILL.md             # 技能文档
└── README.md            # 项目自述文件
```

## 注意事项

- 确保系统安装了uv工具，用于管理Python依赖
- 首次运行必须执行 `initialize-extensions` 命令下载DuckDB内置的数据插件
- 会话ID可以传入当前AI对话ID，用于关联AI对话与数据文件
- 数据文件支持相对路径和绝对路径
- 支持压缩文件的直接加载（如sales.csv.gz）

## 参考资料

- [DuckDB官方文档](https://duckdb.org/docs/)
- [DuckDB SQL参考](https://duckdb.org/docs/sql/introduction)
- [uv官方文档](https://docs.astral.sh/uv/)
