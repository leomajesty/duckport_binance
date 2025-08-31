# DuckPort - 加密货币K线数据服务

基于Arrow Flight协议的加密货币K线数据服务，支持高效的历史数据和实时数据查询传输。

## 功能特性

- 🚀 **高性能数据传输**: 使用Arrow Flight协议进行高效的K线数据传输
- 📊 **K线数据查询**: 支持多种时间间隔的K线数据查询（1-60分钟）
- 🔍 **市场数据管理**: 支持USDT永续合约和现货市场数据
- 🛠️ **智能查询策略**: 自动选择历史数据（Parquet）或实时数据（DuckDB）
- 💾 **混合存储**: 历史数据使用Parquet文件，实时数据使用DuckDB
- ⏰ **时间范围管理**: 支持自定义时间范围和偏移量设置
- 📈 **交易所信息**: 提供完整的交易所和交易对信息

## 安装依赖

```bash
pip install -r requirements.txt
```

## 环境配置

服务需要以下环境配置：

- **时区设置**: 系统时区设置为UTC
- **数据路径**: 配置Parquet文件存储路径
- **数据库配置**: 配置DuckDB数据库路径
- **冗余时间**: 配置数据查询的冗余时间（小时）

### 配置文件说明

系统使用`config.env`文件进行配置，主要配置项如下：

#### 服务配置
- **FLIGHT_PORT**: Flight服务端口，默认值：8815
- **CONCURRENCY**: 获取数据时的并发数，默认值：5

#### 网络配置
- **PROXY_URL**: 代理地址，默认值：http://localhost:1082

#### 存储配置
- **DUCKDB_DIR**: DuckDB数据库文件路径，默认值：data/duckdb.db
- **PARQUET_DIR**: Parquet文件路径，默认值：data/pqt
- **RESOURCE_PATH**: 历史数据资源目录，默认值：data/hist

#### 数据配置
- **REDUNDANCY_HOURS**: 查询范围冗余时间（小时），默认值：1
- **START_MONTH**: 历史数据开始时间，默认值：2025-02
- **KLINE_INTERVAL**: K线周期，默认值：15m
- **RETENTION_DAYS**: DuckDB数据保留时间（天），0表示不清理，默认值：180
- **ENABLE_PQT**: 是否启用PQT文件，默认值：false

## 数据准备

在启动服务之前，需要先加载历史数据。系统提供了`loadhist.py`脚本来下载和处理Binance的历史K线数据。

### 历史数据加载

`loadhist.py`脚本的主要功能：
- 下载Binance的USDT永续合约和现货市场历史数据
- 支持网络连通性测试和代理配置
- 自动处理数据下载、清理和存储转换
- 支持数据过滤（可配置开始时间）

### 数据下载流程

1. **网络测试**: 对fapi.binance.com进行连通性测试
2. **数据获取**: 获取期货和现货交易对列表
3. **目录获取**: 获取历史数据文件目录（daily和monthly）
4. **文件下载**: 并发下载数据文件，支持重试机制
5. **数据清理**: 清理旧的daily数据文件
6. **数据转换**: 将下载的数据转换为Parquet格式或写入DuckDB

### 执行历史数据加载

```bash
# 使用默认配置加载历史数据
python loadhist.py

# 脚本会自动下载以下数据：
# - 第一阶段：USDT永续合约数据
# - 第二阶段：USDT现货数据
```

**注意**: 首次使用系统时，必须先执行`loadhist.py`下载历史数据，否则服务启动后无法提供数据查询功能。

## 快速开始

**重要提示**: 在启动服务之前，请确保已经执行了`loadhist.py`脚本下载了必要的历史数据。如果没有历史数据，服务将无法提供数据查询功能。

### 查询参数说明

- **market**: 市场类型，支持 'usdt_perp'（永续合约）和 'usdt_spot'（现货）
- **interval**: 时间间隔（分钟），支持1-60分钟，且必须是1的倍数
- **offset**: 时间偏移量（分钟），必须小于interval且是1的倍数
- **begin**: 开始时间，默认为90天前
- **end**: 结束时间，默认为当前时间
- **symbol**: 交易对符号（如BTCUSDT）

### 1. 启动服务器

```bash
python start_server.py
```

服务器将在 `grpc://0.0.0.0:8815` 启动。

### 2. 运行客户端示例

```bash
python start_server.py
```

## 服务器功能

### 数据源

服务器提供以下数据表：

1. **usdt_perp表**: USDT永续合约K线数据
   - open_time: 开盘时间
   - symbol: 交易对符号
   - open: 开盘价
   - high: 最高价
   - low: 最低价
   - close: 收盘价
   - volume: 交易量
   - quote_volume: 报价资产交易量
   - trade_num: 交易笔数
   - taker_buy_base_asset_volume: 主动买入基础资产量
   - taker_buy_quote_asset_volume: 主动买入报价资产量
   - avg_price: 平均价格

2. **usdt_spot表**: USDT现货K线数据
   - 字段结构与永续合约表相同

3. **exginfo表**: 交易所信息
   - market: 市场类型
   - symbol: 交易对符号
   - status: 状态
   - base_asset: 基础资产
   - quote_asset: 报价资产
   - price_tick: 价格精度
   - lot_size: 数量精度
   - min_notional_value: 最小名义价值
   - contract_type: 合约类型
   - margin_asset: 保证金资产
   - pre_market: 是否预市
   - created_time: 创建时间

### 支持的Flight操作

1. **list_flights()**: 列出所有可用的数据表
2. **do_get()**: 获取数据，支持：
   - `market` - 获取市场K线数据
   - `symbol` - 获取指定交易对K线数据
   - `exginfo` - 获取交易所信息
3. **get_schema()**: 获取数据表的schema信息
4. **do_action()**: 执行自定义操作：
   - `ping` - 健康检查
   - `ready` - 检查数据准备状态

## 客户端使用示例

```python
from flight.flight_client import FlightClient

# 创建客户端
client = FlightClient()

# 列出可用数据源
flights = client.list_available_data()

# 获取市场K线数据
market_data = client.get_market("usdt_perp", interval=60, offset=0)

# 获取指定交易对K线数据
symbol_data = client.get_symbol("usdt_perp", "BTCUSDT", interval=15, offset=0)

# 获取交易所信息
exginfo_data = client.get_exginfo("usdt_perp")

# 健康检查
response = client.ping()

# 检查数据准备状态
ready_status = client.ready("usdt_perp")

# 获取schema
schema = client.get_schema("usdt_perp")
```

## API参考

### FlightServer

主要的服务器类，继承自 `flight.FlightServerBase`。

#### 方法

- `__init__(db_manager, location, pqt_path)`: 初始化服务器
- `do_get(context, ticket)`: 处理数据获取请求
- `list_flights(context, criteria)`: 列出可用数据表
- `get_schema(context, descriptor)`: 获取表schema信息
- `do_action(context, action)`: 处理自定义操作
- `list_actions(context)`: 列出支持的操作

### FlightActions

处理自定义操作的内部类。

#### 方法

- `action_ping()`: 健康检查操作
- `action_ready(market)`: 检查指定市场的数据准备状态

### FlightGets

处理数据获取的内部类。

#### 方法

- `get_market(market, interval, offset, begin, end)`: 获取市场K线数据
- `get_symbol(market, symbol, interval, offset, begin, end)`: 获取指定交易对K线数据
- `get_exginfo(market)`: 获取交易所信息

### FlightClient

客户端类，用于连接和查询Flight服务器。

#### 方法

- `__init__(host, port)`: 初始化客户端连接
- `list_available_data()`: 列出可用数据源
- `get_schema(table_name)`: 获取表schema
- `ping()`: 健康检查
- `ready(market)`: 检查数据准备状态
- `list_actions()`: 列出所有可用的actions
- `get_market(market, interval, offset, begin, end)`: 获取市场K线数据
- `get_symbol(market, symbol, interval, offset, begin, end)`: 获取指定交易对K线数据
- `get_exginfo(market)`: 获取交易所信息
- `close()`: 关闭客户端连接

## 协议支持

本服务器完全支持Arrow Flight协议，包括：

- ✅ Flight RPC服务
- ✅ 数据流传输
- ✅ 元数据管理
- ✅ 自定义操作
- ✅ Schema信息
- ✅ 混合存储策略

## 性能特点

- **高效传输**: 使用Arrow列式格式，减少网络传输量
- **智能查询**: 自动选择最优数据源（Parquet历史数据或DuckDB实时数据）
- **时间范围优化**: 支持自定义时间间隔和偏移量，优化查询性能
- **混合存储**: 历史数据使用Parquet文件，实时数据使用DuckDB，平衡存储成本和查询性能

## 扩展性

服务器设计具有良好的扩展性：

1. **添加新市场**: 在 `_init_database()` 方法中添加新的市场表
2. **自定义操作**: 在 `FlightActions` 类中添加新的action类型
3. **数据源适配**: 可以轻松适配不同的数据源（如PostgreSQL、MySQL等）
4. **查询策略扩展**: 在 `FlightGets` 类中添加新的查询类型
5. **数据源扩展**: 在`loadhist.py`中添加新的交易所或市场类型支持
6. **存储格式扩展**: 支持其他数据格式的导入和转换
7. **下载策略优化**: 可以自定义数据下载的时间间隔和过滤条件

## 故障排除

### 常见问题

1. **连接失败**: 确保服务器已启动且端口8815可访问
2. **查询参数错误**: 检查市场类型、时间间隔、偏移量等参数是否正确
3. **数据范围错误**: 确保查询的时间范围在可用数据范围内
4. **市场类型错误**: 支持的市场类型为 'usdt_perp' 和 'usdt_spot'
5. **无数据返回**: 确保已经执行`loadhist.py`下载了历史数据
6. **网络连接问题**: 检查代理配置和网络连通性，确保可以访问Binance API
7. **存储空间不足**: 确保有足够的磁盘空间存储历史数据文件

### 日志

服务器和客户端都配置了详细的日志输出，可以通过日志信息进行调试。

## 许可证

本项目基于Apache 2.0许可证开源。

## 技术架构

- **后端**: Python + Arrow Flight + DuckDB
- **数据格式**: Apache Arrow
- **存储**: Parquet文件 + DuckDB数据库
- **协议**: gRPC over HTTP/2
- **时区**: UTC统一时区
- **数据源**: Binance API (期货和现货市场)
- **下载策略**: 异步并发下载 + 重试机制
- **数据处理**: 批量处理 + 自动格式转换 