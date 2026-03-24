# 多数据源数据管理系统

支持多个数据源的金融数据采集和管理系统。

## 项目结构

```
├── .env                          # 环境变量配置（敏感信息）
├── .env.example                  # 环境变量示例
├── main.py                       # 系统入口
├── requirements.txt              # 依赖包
├── src/
│   ├── common/                   # 共享模块
│   │   ├── config.py            # 配置管理
│   │   ├── database.py          # 数据库
│   │   ├── logger.py            # 日志
│   │   ├── models.py            # 数据模型
│   │   └── retry.py             # 重试机制
│   ├── collectors/              # 数据采集器
│   │   ├── __init__.py          # 基类定义
│   │   ├── manager.py           # 采集器管理
│   │   ├── starlight/           # Starlight 采集器
│   │   │   ├── client.py
│   │   │   └── scheduler.py
│   │   └── miniqmt/             # MiniQMT 采集器
│   │       ├── client.py
│   │       ├── database.py
│   │       └── scheduler.py
│   └── services/                # 服务层
│       ├── api.py               # REST API
│       ├── mcp.py               # MCP 服务
│       └── qmt_mcp.py           # QMT MCP
└── tests/                       # 测试
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

复制 `.env.example` 为 `.env` 并填入你的配置：

```bash
cp .env.example .env
```

编辑 `.env` 文件，填入敏感信息。

### 3. 启动系统

```bash
# 启动完整服务（所有数据源）
python main.py

# 仅使用 Starlight
python main.py --source starlight

# 仅使用 MiniQMT
python main.py --source miniqmt

# 仅启动 API（不启动定时任务）
python main.py --no-scheduler

# 仅 API 模式
python main.py --mode api
```

## 数据源

### Starlight
- 位置：`src/collectors/starlight/`
- 配置：`.env` 中的 `AD_*` 变量
- 数据库：ClickHouse

### MiniQMT
- 位置：`src/collectors/miniqmt/`
- 配置：`.env` 中的 `QMT_*` 变量
- 数据库：ClickHouse

## 添加新数据源

1. 在 `src/collectors/` 下创建新目录
2. 实现 `BaseCollector` 接口
3. 在 `main.py` 中注册新数据源

## API 文档

启动后访问：http://localhost:8000/docs

## 环境变量

查看 `.env.example` 了解所有可配置项。
