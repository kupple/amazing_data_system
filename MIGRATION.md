# 项目重构迁移指南

## 重构内容

### 1. 敏感信息保护
- 所有敏感配置已移至 `.env` 文件
- 添加 `.env.example` 作为配置模板
- `.env` 已加入 `.gitignore`

### 2. 项目结构重组

**旧结构 → 新结构：**

```
src/config.py          → src/common/config.py
src/logger.py          → src/common/logger.py
src/database.py        → src/common/database.py
src/retry.py           → src/common/retry.py
src/models.py          → src/common/models.py

src/client.py          → src/collectors/amazingdata/client.py
src/scheduler.py       → src/collectors/amazingdata/scheduler.py

src/qmt/client.py      → src/collectors/qmt/client.py
src/qmt/database.py    → src/collectors/qmt/database.py
src/qmt/scheduler.py   → src/collectors/qmt/scheduler.py
src/qmt/mcp.py         → src/services/qmt_mcp.py

src/api.py             → src/services/api.py
src/mcp.py             → src/services/mcp.py
```

### 3. 导入路径更新

所有导入已更新为新路径：
- `from src.config import` → `from src.common.config import`
- `from src.logger import` → `from src.common.logger import`
- `from src.database import` → `from src.common.database import`
- `from src.retry import` → `from src.common.retry import`
- `from src.models import` → `from src.common.models import`
- `from src.client import` → `from src.collectors.amazingdata.client import`
- `from src.qmt.client import` → `from src.collectors.qmt.client import`

### 4. 新增功能

- **数据源管理器** (`src/collectors/manager.py`)
  - 统一管理多个数据源
  - 支持动态切换数据源
  - 支持同时连接多个数据源

- **基类定义** (`src/collectors/__init__.py`)
  - `BaseCollector` 抽象基类
  - 标准化数据采集器接口

## 使用方法

### 配置环境变量

```bash
cp .env.example .env
# 编辑 .env 填入你的配置
```

### 启动系统

```bash
# 使用所有数据源
python main.py

# 仅使用 AmazingData
python main.py --source amazingdata

# 仅使用 QMT
python main.py --source qmt
```

## 注意事项

1. 确保 `.env` 文件已正确配置
2. 旧的导入路径已全部更新
3. 测试文件可能需要手动调整
