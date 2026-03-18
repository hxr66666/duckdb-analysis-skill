import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import datetime
import argparse
import uuid

class DuckDBAnalysis:
    def __init__(self, session_id):
        # 使用传入的会话ID
        self.session_id = session_id
        self.db_name = f"session_{self.session_id}.db"
        
        
        # 使用绝对路径构建数据库路径
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_dir = os.path.dirname(script_dir)
        data_dir = os.path.join(project_dir, "data")
        # 确保数据目录存在
        if not os.path.exists(data_dir):
            os.makedirs(data_dir, exist_ok=True)

        self.db_path = os.path.join(data_dir, self.db_name)
        # 连接到当前会话的数据库
        self.conn = duckdb.connect(self.db_path)
        
        # 只尝试加载扩展，不安装（安装过程在initialize_extensions方法中）
        try:
            # 加载Excel扩展
            self.conn.execute("LOAD excel")
            # 加载HTTP扩展（用于远程文件）
            self.conn.execute("LOAD httpfs")
            # 加载编码扩展
            self.conn.execute("LOAD encodings")
            # 加载JSON扩展
            self.conn.execute("LOAD json")
            # 加载Parquet扩展
            self.conn.execute("LOAD parquet")
            # 加载FTS扩展
            self.conn.execute("LOAD fts")
            # 加载INET扩展
            self.conn.execute("LOAD inet")
        except Exception as e:
            print(f"加载扩展时出错: {str(e)}")
            print("请运行 initialize_extensions 命令来安装所有必要的扩展")
        
        # 连接到元数据库（analysis.db）
        self.meta_db_path = os.path.join(data_dir, "analysis.db")
        self.meta_conn = duckdb.connect(self.meta_db_path)
        
        # 初始化元数据库表
        self._init_meta_db()
        
        self.tables = {}
        self.session_description = ""
        
        # 记录新会话
        self._record_session()
    
    def initialize_extensions(self):
        """
        初始化扩展（第一次运行时下载和安装所有必要的扩展）
        这个过程可能需要较长时间，因为需要从网络下载扩展
        """
        try:
            # 安装Excel扩展
            print("正在安装Excel扩展...")
            self.conn.execute("INSTALL excel")
            # 安装HTTP扩展（用于远程文件）
            print("正在安装HTTP扩展...")
            self.conn.execute("INSTALL httpfs")
            # 安装编码扩展
            print("正在安装编码扩展...")
            self.conn.execute("INSTALL encodings")
            # 安装FTS扩展
            print("正在安装FTS扩展...")
            self.conn.execute("INSTALL fts")
            # 安装INET扩展
            print("正在安装INET扩展...")
            self.conn.execute("INSTALL inet")
            
            # 加载所有扩展
            print("正在加载扩展...")
            self.conn.execute("LOAD excel")
            self.conn.execute("LOAD httpfs")
            self.conn.execute("LOAD encodings")
            self.conn.execute("LOAD json")
            self.conn.execute("LOAD parquet")
            self.conn.execute("LOAD fts")
            self.conn.execute("LOAD inet")
            
            return "扩展初始化完成"
        except Exception as e:
            return f"初始化扩展时出错: {str(e)}"
    
    def _init_meta_db(self):
        """
        初始化元数据库表
        """
        # 创建会话表
        self.meta_conn.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_id VARCHAR PRIMARY KEY,
            db_name VARCHAR,
            created_at TIMESTAMP,
            description VARCHAR
        )
        """)
        
        # 创建数据表
        self.meta_conn.execute("""
        CREATE TABLE IF NOT EXISTS session_data (
            id INTEGER PRIMARY KEY,
            session_id VARCHAR,
            table_name VARCHAR,
            file_path VARCHAR,
            imported_at TIMESTAMP,
            FOREIGN KEY (session_id) REFERENCES sessions(session_id)
        )
        """)
        
        # 创建操作日志表
        self.meta_conn.execute("""
        CREATE TABLE IF NOT EXISTS operation_logs (
            id INTEGER PRIMARY KEY,
            session_id VARCHAR,
            timestamp TIMESTAMP,
            action VARCHAR,
            details VARCHAR,
            status VARCHAR,
            error VARCHAR,
            file_path VARCHAR,
            FOREIGN KEY (session_id) REFERENCES sessions(session_id)
        )
        """)
    
    def _record_session(self):
        """
        记录新会话到元数据库
        """
        created_at = datetime.datetime.now().isoformat()
        
        # 检查会话是否已存在
        existing_session = self.meta_conn.execute(
            "SELECT session_id FROM sessions WHERE session_id = ?",
            [self.session_id]
        ).fetchone()
        
        if existing_session:
            # 更新现有会话
            self.meta_conn.execute(
                "UPDATE sessions SET db_name = ?, created_at = ?, description = ? WHERE session_id = ?",
                [self.db_name, created_at, self.session_description, self.session_id]
            )
        else:
            # 插入新会话
            self.meta_conn.execute(
                "INSERT INTO sessions (session_id, db_name, created_at, description) VALUES (?, ?, ?, ?)",
                [self.session_id, self.db_name, created_at, self.session_description]
            )
    
    def _record_operation(self, operation):
        """
        记录操作到元数据库
        """
        try:
            # 生成ID
            max_id = self.meta_conn.execute("SELECT COALESCE(MAX(id), 0) FROM operation_logs").fetchone()[0]
            new_id = max_id + 1
            
            # 构建详细信息
            details = {}
            if 'sql' in operation:
                details['sql'] = operation['sql']
            if 'chart_type' in operation:
                details['chart_type'] = operation['chart_type']
            if 'x' in operation:
                details['x'] = operation['x']
            if 'y' in operation:
                details['y'] = operation['y']
            if 'hue' in operation:
                details['hue'] = operation['hue']
            if 'file_path' in operation:
                details['file_path'] = operation['file_path']
            if 'table_name' in operation:
                details['table_name'] = operation['table_name']
            if 'file_path' in operation:
                details['file_path'] = operation['file_path']
            
            import json
            details_json = json.dumps(details)
            
            # 插入操作日志
            self.meta_conn.execute(
                "INSERT INTO operation_logs (id, session_id, timestamp, action, details, status, error, file_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    new_id,
                    self.session_id,
                    operation['timestamp'],
                    operation['action'],
                    details_json,
                    operation['status'],
                    operation.get('error', None),
                    operation.get('file_path', None)
                ]
            )
        except Exception as e:
            print(f"记录操作日志时出错: {str(e)}")
    
    def set_session_description(self, description):
        """
        设置会话描述
        """
        self.session_description = description
        self.meta_conn.execute(
            "UPDATE sessions SET description = ? WHERE session_id = ?",
            [description, self.session_id]
        )
        return f"会话描述已更新: {description}"
    
    def import_data(self, file_path, table_name):
        """
        导入数据文件到duckdb表中
        支持的文件格式：csv、excel、parquet、json、jsonl
        """
        file_ext = os.path.splitext(file_path)[1].lower()
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        imported_at = datetime.datetime.now().isoformat()
        
        try:
            if file_ext == '.csv':
                # 使用官方推荐的CSV加载方式
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv('{file_path}')")
            elif file_ext in ['.xlsx', '.xls']:
                # 使用Excel扩展加载Excel文件
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_xlsx('{file_path}')")
            elif file_ext == '.parquet':
                # 使用官方推荐的Parquet加载方式
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')")
            elif file_ext == '.json':
                # 使用官方推荐的JSON加载方式
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{file_path}')")
            elif file_ext == '.jsonl':
                # 使用官方推荐的JSONL加载方式
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{file_path}')")
            else:
                return f"不支持的文件格式: {file_ext}"
            
            # 生成ID
            max_id = self.meta_conn.execute("SELECT COALESCE(MAX(id), 0) FROM session_data").fetchone()[0]
            new_id = max_id + 1
            
            # 记录到元数据库
            self.meta_conn.execute(
                "INSERT INTO session_data (id, session_id, table_name, file_path, imported_at) VALUES (?, ?, ?, ?, ?)",
                [new_id, self.session_id, table_name, file_path, imported_at]
            )
            
            # 记录操作
            operation = {
                "timestamp": timestamp,
                "action": "import_data",
                "file_path": file_path,
                "table_name": table_name,
                "status": "success"
            }
            # 记录到元数据库
            self._record_operation(operation)
            
            self.tables[table_name] = file_path
            return f"数据导入成功，创建表: {table_name}"
        except Exception as e:
            # 记录失败操作
            operation = {
                "timestamp": timestamp,
                "action": "import_data",
                "file_path": file_path,
                "table_name": table_name,
                "status": "failed",
                "error": str(e)
            }
            # 记录到元数据库
            self._record_operation(operation)
            return f"导入失败: {str(e)}"
    
    def list_tables(self):
        """
        列出所有已导入的表
        """
        try:
            # 从数据库中获取实际存在的表
            tables_info = self.conn.execute("SHOW TABLES").fetchall()
            
            if not tables_info:
                return "没有已导入的表"
            
            result = "已导入的表:\n"
            for table_info in tables_info:
                table_name = table_info[0]
                result += f"- {table_name}\n"
            return result
        except Exception as e:
            return f"获取表列表失败: {str(e)}"
    
    def table_metadata(self, table_name):
        """
        获取表的元数据（字段信息）
        """
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            # 使用PRAGMA table_info获取表的字段信息
            metadata = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            if not metadata:
                # 记录操作
                operation = {
                    "timestamp": timestamp,
                    "action": "table_metadata",
                    "table_name": table_name,
                    "status": "failed",
                    "error": f"表 {table_name} 不存在或没有字段信息"
                }
                # 记录到元数据库
                self._record_operation(operation)
                return f"表 {table_name} 不存在或没有字段信息"
            
            result = f"表 {table_name} 的字段信息:\n"
            result += "字段名 | 数据类型 | 是否可为空 | 默认值 | 主键\n"
            result += "-" * 60 + "\n"
            
            for col in metadata:
                col_id, name, type, notnull, dflt_value, pk = col
                notnull_str = "否" if notnull else "是"
                pk_str = "是" if pk else "否"
                dflt_value_str = dflt_value if dflt_value is not None else "无"
                result += f"{name} | {type} | {notnull_str} | {dflt_value_str} | {pk_str}\n"
            
            # 记录操作
            operation = {
                "timestamp": timestamp,
                "action": "table_metadata",
                "table_name": table_name,
                "status": "success"
            }
            # 记录到元数据库
            self._record_operation(operation)
            
            return result
        except Exception as e:
            # 记录失败操作
            operation = {
                "timestamp": timestamp,
                "action": "table_metadata",
                "table_name": table_name,
                "status": "failed",
                "error": str(e)
            }
            # 记录到元数据库
            self._record_operation(operation)
            return f"获取表元数据时出错: {str(e)}"
    
    def export_csv(self, sql, output_path):
        """
        执行SQL查询并将结果导出为CSV文件
        使用DuckDB的内置功能直接导出
        """
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            # 确保输出目录存在
            output_dir = os.path.dirname(output_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            
            # 使用DuckDB的内置功能直接导出为CSV
            # 构建导出SQL语句
            export_sql = f"COPY ({sql}) TO '{output_path}' (HEADER, DELIMITER ',', ENCODING 'UTF8')"
            self.conn.execute(export_sql)
            
            # 获取结果行数（可选）
            result_count = self.conn.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]
            
            # 记录操作
            operation = {
                "timestamp": timestamp,
                "action": "export_csv",
                "sql": sql,
                "file_path": output_path,
                "status": "success",
                "result_rows": result_count
            }
            # 记录到元数据库
            self._record_operation(operation)
            
            return f"查询结果已成功导出到: {output_path}"
        except Exception as e:
            # 记录失败操作
            operation = {
                "timestamp": timestamp,
                "action": "export_csv",
                "sql": sql,
                "file_path": output_path,
                "status": "failed",
                "error": str(e)
            }
            # 记录到元数据库
            self._record_operation(operation)
            return f"导出CSV失败: {str(e)}"
    
    def query(self, sql):
        """
        执行SQL查询并返回markdown格式的表格结果
        """
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            result = self.conn.execute(sql).fetchall()
            columns = [desc[0] for desc in self.conn.execute(sql).description]
            df = pd.DataFrame(result, columns=columns)
            
            # 记录操作
            operation = {
                "timestamp": timestamp,
                "action": "query",
                "sql": sql,
                "status": "success",
                "result_rows": len(df)
            }
            # 记录到元数据库
            self._record_operation(operation)
            
            # 将DataFrame转换为markdown格式的表格
            if len(df) == 0:
                return "查询结果为空"
            
            # 生成markdown表格
            markdown_table = "| " + " | ".join(columns) + " |\n"
            markdown_table += "| " + " | ".join(["---"] * len(columns)) + " |\n"
            
            for _, row in df.iterrows():
                row_values = []
                for val in row:
                    # 处理空值
                    if pd.isna(val):
                        row_values.append("无")
                    else:
                        # 转换为字符串，确保格式正确
                        row_values.append(str(val))
                markdown_table += "| " + " | ".join(row_values) + " |\n"
            
            return markdown_table
        except Exception as e:
            # 记录失败操作
            operation = {
                "timestamp": timestamp,
                "action": "query",
                "sql": sql,
                "status": "failed",
                "error": str(e)
            }
            # 记录到元数据库
            self._record_operation(operation)
            return f"查询失败: {str(e)}"
    
    def visualize(self, sql, chart_type='bar', x=None, y=None, hue=None):
        """
        执行SQL查询并可视化结果
        支持的图表类型: bar, line, scatter, histogram, box
        """
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            df = self.query(sql)
            if isinstance(df, str):
                # 记录失败操作
                operation = {
                    "timestamp": timestamp,
                    "action": "visualize",
                    "sql": sql,
                    "chart_type": chart_type,
                    "x": x,
                    "y": y,
                    "hue": hue,
                    "status": "failed",
                    "error": df
                }
                self.operation_log.append(operation)
                return df
            
            plt.figure(figsize=(10, 6))
            
            if chart_type == 'bar':
                if x and y:
                    sns.barplot(data=df, x=x, y=y, hue=hue)
                else:
                    error_msg = "需要指定x和y参数"
                    operation = {
                        "timestamp": timestamp,
                        "action": "visualize",
                        "sql": sql,
                        "chart_type": chart_type,
                        "x": x,
                        "y": y,
                        "hue": hue,
                        "status": "failed",
                        "error": error_msg
                    }
                    self.operation_log.append(operation)
                    return error_msg
            elif chart_type == 'line':
                if x and y:
                    sns.lineplot(data=df, x=x, y=y, hue=hue)
                else:
                    error_msg = "需要指定x和y参数"
                    operation = {
                        "timestamp": timestamp,
                        "action": "visualize",
                        "sql": sql,
                        "chart_type": chart_type,
                        "x": x,
                        "y": y,
                        "hue": hue,
                        "status": "failed",
                        "error": error_msg
                    }
                    self.operation_log.append(operation)
                    return error_msg
            elif chart_type == 'scatter':
                if x and y:
                    sns.scatterplot(data=df, x=x, y=y, hue=hue)
                else:
                    error_msg = "需要指定x和y参数"
                    operation = {
                        "timestamp": timestamp,
                        "action": "visualize",
                        "sql": sql,
                        "chart_type": chart_type,
                        "x": x,
                        "y": y,
                        "hue": hue,
                        "status": "failed",
                        "error": error_msg
                    }
                    self.operation_log.append(operation)
                    return error_msg
            elif chart_type == 'histogram':
                if x:
                    sns.histplot(data=df, x=x, hue=hue)
                else:
                    error_msg = "需要指定x参数"
                    operation = {
                        "timestamp": timestamp,
                        "action": "visualize",
                        "sql": sql,
                        "chart_type": chart_type,
                        "x": x,
                        "y": y,
                        "hue": hue,
                        "status": "failed",
                        "error": error_msg
                    }
                    self.operation_log.append(operation)
                    return error_msg
            elif chart_type == 'box':
                if x and y:
                    sns.boxplot(data=df, x=x, y=y, hue=hue)
                else:
                    error_msg = "需要指定x和y参数"
                    operation = {
                        "timestamp": timestamp,
                        "action": "visualize",
                        "sql": sql,
                        "chart_type": chart_type,
                        "x": x,
                        "y": y,
                        "hue": hue,
                        "status": "failed",
                        "error": error_msg
                    }
                    self.operation_log.append(operation)
                    return error_msg
            else:
                error_msg = f"不支持的图表类型: {chart_type}"
                operation = {
                    "timestamp": timestamp,
                    "action": "visualize",
                    "sql": sql,
                    "chart_type": chart_type,
                    "x": x,
                    "y": y,
                    "hue": hue,
                    "status": "failed",
                    "error": error_msg
                }
                self.operation_log.append(operation)
                return error_msg
            
            plt.title('数据可视化')
            plt.tight_layout()

            # 创建img文件夹（如果不存在）
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_dir = os.path.dirname(script_dir)
            img_dir = os.path.join(project_dir, "img")
            if not os.path.exists(img_dir):
                os.makedirs(img_dir, exist_ok=True)

            # 生成随机文件名
            random_filename = f"visualization_{uuid.uuid4().hex[:8]}.png"
            # 使用绝对路径保存可视化结果，便于AI读取
            visualization_path = os.path.abspath(os.path.join(img_dir, random_filename))
            plt.savefig(visualization_path)
            
            # 记录成功操作
            operation = {
                "timestamp": timestamp,
                "action": "visualize",
                "sql": sql,
                "chart_type": chart_type,
                "x": x,
                "y": y,
                "hue": hue,
                "status": "success",
                "file_path": visualization_path
            }
            # 记录到元数据库
            self._record_operation(operation)
            
            return f"可视化成功，保存为: {visualization_path}"
        except Exception as e:
            # 记录失败操作
            operation = {
                "timestamp": timestamp,
                "action": "visualize",
                "sql": sql,
                "chart_type": chart_type,
                "x": x,
                "y": y,
                "hue": hue,
                "status": "failed",
                "error": str(e)
            }
            # 记录到元数据库
            self._record_operation(operation)
            return f"可视化失败: {str(e)}"
    
    def close(self):
        """
        关闭数据库连接
        """
        self.conn.close()
        self.meta_conn.close()
        return f"数据库连接已关闭，会话ID: {self.session_id}"
    
    def list_sessions(self):
        """
        列出所有会话信息
        """
        try:
            sessions = self.meta_conn.execute("SELECT session_id, db_name, created_at, description FROM sessions ORDER BY created_at DESC").fetchall()
            
            if not sessions:
                return "没有会话记录"
            
            result = "所有会话:\n"
            for session in sessions:
                session_id, db_name, created_at, description = session
                result += f"- 会话ID: {session_id}\n"
                result += f"  数据库文件: {db_name}\n"
                result += f"  创建时间: {created_at}\n"
                result += f"  描述: {description or '无'}\n"
                
                # 列出该会话导入的数据
                data_imports = self.meta_conn.execute(
                    "SELECT table_name, file_path, imported_at FROM session_data WHERE session_id = ?",
                    [session_id]
                ).fetchall()
                
                if data_imports:
                    result += "  导入的数据:\n"
                    for data in data_imports:
                        table_name, file_path, imported_at = data
                        result += f"    - 表名: {table_name}, 文件: {file_path}, 导入时间: {imported_at}\n"
                else:
                    result += "  导入的数据: 无\n"
                result += "\n"
            return result
        except Exception as e:
            return f"获取会话列表失败: {str(e)}"
    
    def show_operation_log(self):
        """
        查看操作日志（从元数据库中查询）
        """
        try:
            # 从元数据库中查询当前会话的操作日志
            logs = self.meta_conn.execute(
                "SELECT timestamp, action, details, status, error, file_path FROM operation_logs WHERE session_id = ? ORDER BY timestamp DESC",
                [self.session_id]
            ).fetchall()
            
            if not logs:
                return "操作日志为空"
            
            result = "操作日志:\n"
            import json
            for i, log in enumerate(logs, 1):
                timestamp, action, details, status, error, file_path = log
                result += f"{i}. [{timestamp}] {action} - {status}\n"
                
                # 解析详细信息
                if details:
                    try:
                        details_dict = json.loads(details)
                        if action == 'import_data':
                            if 'file_path' in details_dict:
                                result += f"   文件: {details_dict['file_path']}\n"
                            if 'table_name' in details_dict:
                                result += f"   表名: {details_dict['table_name']}\n"
                        elif action == 'query':
                            if 'sql' in details_dict:
                                result += f"   SQL: {details_dict['sql']}\n"
                        elif action == 'visualize':
                            if 'sql' in details_dict:
                                result += f"   SQL: {details_dict['sql']}\n"
                            if 'chart_type' in details_dict:
                                result += f"   图表类型: {details_dict['chart_type']}\n"
                            x = details_dict.get('x', '无')
                            y = details_dict.get('y', '无')
                            hue = details_dict.get('hue', '无')
                            result += f"   x轴: {x}, y轴: {y}, 分组: {hue}\n"
                    except:
                        pass
                
                if file_path:
                    result += f"   文件: {file_path}\n"
                if status == 'failed' and error:
                    result += f"   错误: {error}\n"
                result += "\n"
            return result
        except Exception as e:
            return f"查询操作日志时出错: {str(e)}"

def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='DuckDB 数据分析工具')
    parser.add_argument('--session-id', required=True, help='会话ID，可以传入当前ai对话id用于关联当前ai对话与数据文件一对一绑定。也可以是个随机字符串')
    
    # 子命令
    subparsers = parser.add_subparsers(dest='command', help='操作命令')
    
    # 0. 初始化扩展（第一次运行时使用）
    init_ext_parser = subparsers.add_parser('initialize-extensions', help='初始化扩展（第一次运行时下载和安装所有必要的扩展）')
    
    # 1. 导入数据
    import_parser = subparsers.add_parser('import', help='导入数据')
    import_parser.add_argument('--file', required=True, help='数据文件路径')
    import_parser.add_argument('--table', required=True, help='表名')
    import_parser.add_argument('--description', help='会话描述（可选）')
    
    # 2. 查看已导入的表
    list_tables_parser = subparsers.add_parser('list-tables', help='查看已导入的表')
    
    # 3. 获取表元数据
    table_metadata_parser = subparsers.add_parser('table-metadata', help='获取表的元数据（字段信息）')
    table_metadata_parser.add_argument('--table', required=True, help='表名')
    
    # 4. 执行SQL查询
    query_parser = subparsers.add_parser('query', help='执行SQL查询')
    query_parser.add_argument('--sql', required=True, help='SQL查询语句')
    
    # 5. 数据可视化
    visualize_parser = subparsers.add_parser('visualize', help='数据可视化')
    visualize_parser.add_argument('--sql', required=True, help='SQL查询语句')
    visualize_parser.add_argument('--chart', required=True, choices=['bar', 'line', 'scatter', 'histogram', 'box'], help='图表类型')
    visualize_parser.add_argument('--x', required=True, help='x轴字段')
    visualize_parser.add_argument('--y', help='y轴字段（直方图不需要）')
    
    # 6. 查看操作日志
    log_parser = subparsers.add_parser('log', help='查看操作日志')
    
    # 7. 设置会话描述
    describe_parser = subparsers.add_parser('describe', help='设置会话描述')
    describe_parser.add_argument('--description', required=True, help='会话描述')
    
    # 8. 查看所有会话
    list_sessions_parser = subparsers.add_parser('list-sessions', help='查看所有会话')
    
    # 9. 导出查询结果为CSV
    export_csv_parser = subparsers.add_parser('export-csv', help='导出查询结果为CSV文件')
    export_csv_parser.add_argument('--sql', required=True, help='SQL查询语句')
    export_csv_parser.add_argument('--output', required=True, help='输出CSV文件路径')
    
    args = parser.parse_args()
    
    analyzer = DuckDBAnalysis(args.session_id)
    
    try:
        if args.command == 'initialize-extensions':
            result = analyzer.initialize_extensions()
            print(result)
        elif args.command == 'import':
            result = analyzer.import_data(args.file, args.table)
            print(result)
            # 如果提供了描述参数，设置会话描述
            if args.description:
                desc_result = analyzer.set_session_description(args.description)
                print(desc_result)
        elif args.command == 'list-tables':
            result = analyzer.list_tables()
            print(result)
        elif args.command == 'table-metadata':
            result = analyzer.table_metadata(args.table)
            print(result)
        elif args.command == 'query':
            result = analyzer.query(args.sql)
            print(result)
        elif args.command == 'visualize':
            if args.chart == 'histogram':
                result = analyzer.visualize(args.sql, args.chart, args.x, None, args.hue)
            else:
                if not args.y:
                    print("错误: 除直方图外，其他图表类型需要指定y轴字段")
                    return
                result = analyzer.visualize(args.sql, args.chart, args.x, args.y, args.hue)
            print(result)
        elif args.command == 'log':
            result = analyzer.show_operation_log()
            print(result)
        elif args.command == 'describe':
            result = analyzer.set_session_description(args.description)
            print(result)
        elif args.command == 'list-sessions':
            result = analyzer.list_sessions()
            print(result)
        elif args.command == 'export-csv':
            result = analyzer.export_csv(args.sql, args.output)
            print(result)
        else:
            parser.print_help()
    finally:
        analyzer.close()

if __name__ == "__main__":
    main()
