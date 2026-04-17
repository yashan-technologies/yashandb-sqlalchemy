# YashanDB SQLAlchemy 方言 — 用户指南

本文档说明如何安装依赖、安装本包，以及如何在应用中使用 `yashandb_sqlalchemy` 连接 YashanDB。

根目录下的 `README.md` 目前主要为 GitLab 模板占位；**实际使用请以本文件为准**。

---

## 1. 环境要求

| 组件 | 说明 |
|------|------|
| Python | 建议 **3.9.x**（与当前验证环境一致；其它 3.x 版本需自行验证） |
| SQLAlchemy | 建议 **1.4.0**（比如SQLAlchemy1.4.5，本仓库开发与方言测试套件验证版本） |
| 数据库 | YashanDB |
| Python 驱动 | **yaspy** |

> **驱动安装**：`yaspy` 已可通过 PyPI 安装（`pip install yaspy`）。本仓库 **`setup.py` 未将 `yaspy` 列为安装依赖**（运行应用前仍需自行安装 `yaspy`）。开发/跑连库测试时，执行 `pip install -r requirements_dev.txt` 会一并安装 `yaspy`（文件中未钉死版本号时，由 pip 从当前索引解析并安装最新兼容版本）。

---

## 2. 依赖说明

### 2.1 运行时依赖

- **SQLAlchemy**：ORM/Core 与方言接口所依赖。
- **与数据库通信的 DBAPI 驱动**（如 `yaspy`）：运行应用时需自行安装；`requirements_dev.txt` 中已包含 `yaspy`，便于测试环境一键安装。

本仓库包名：`yashandb-sqlalchemy`（见 `setup.py` 中 `name`）。

### 2.2 开发与跑官方测试套件（可选）

若需运行 `pytest` + SQLAlchemy 方言测试插件，可使用仓库中的固定版本文件：

```bash
pip install -r requirements_dev.txt
```

其中包含例如 `SQLAlchemy==1.4.5`、`pytest==6.2.5`、`yaspy` 等（详见 `requirements_dev.txt`；`yaspy` 未写版本号时由 pip 选取索引上的可用版本）。

---

## 3. 安装本包

### 3.1 从源码目录安装（推荐用于开发）

在克隆后的仓库根目录执行：

```bash
pip install -e .
```

### 3.2 从 Git 地址安装（用于部署）

```bash
pip install "git+https://git.yasdb.com/cod-x/sqlalchemy-yasdb.git@<分支或标签>#egg=yashandb-sqlalchemy"
```

将 `<分支或标签>` 换成实际分支名或 tag。

### 3.3 版本号说明

`setup.py` 中版本通过 **`git describe --tags`** 生成。请保证仓库存在合理 **git tag**，否则可能得到非 PEP440 的版本字符串（例如 `unknow`），影响 `pip install` / 打 wheel。

---

## 4. 方言注册与连接 URL

安装后，SQLAlchemy 会通过 `setup.py` 里的 **entry_points** 注册方言（无需手写 `registry.register`，除非你在特殊场景下绕过安装）：

| URL 前缀 | 说明 |
|----------|------|
| `yashandb+yaspy://...` | 使用 **yaspy** 驱动（**推荐**） |
| `yashandb+yasdb://...` | 使用 **yasdb** 驱动 |
| `yashandb://...` | 默认映射到 **yaspy**（与 entry point `yashandb = ...yaspy` 一致） |

URL 一般形式（请按实际主机、端口、库名、用户修改）：

```text
yashandb+yaspy://用户名:密码@主机:端口/数据库或服务名
```

示例（占位符，勿直接复制到生产）：

```text
yashandb+yaspy://sys:你的密码@127.0.0.1:1688/test
```

---

## 5. 使用示例

### 5.1 Engine / 执行 SQL（Core）

```python
from sqlalchemy import create_engine, text

engine = create_engine("yashandb+yaspy://用户:密码@主机:端口/库名")

with engine.connect() as conn:
    result = conn.execute(text("SELECT 1 FROM DUAL"))
    print(result.scalar())
```

### 5.2 声明式 ORM（SQLAlchemy 1.4 风格）

```python
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String(50))

engine = create_engine("yashandb+yaspy://用户:密码@主机:端口/库名")
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()
# ... 业务代码 ...
session.close()
```

### 5.3 方言专有类型（可选）

部分 Oracle 兼容类型可从包顶层导入，例如：

```python
from yashandb_sqlalchemy import BINARY_DOUBLE, VARCHAR2
```

类型定义位于 `yashandb_sqlalchemy/base.py`，并在 `yashandb_sqlalchemy/__init__.py` 中再导出。

---

## 6. 重要说明：yaspy 与线程 / 连接池

**yaspy 驱动在同一连接上通常只应在单线程内使用**；本方言对 **yaspy** 使用了 `SingletonThreadPool`（见 `yashandb_sqlalchemy/yaspy.py`），以尽量符合该限制。

应用侧请注意：

- 避免多个线程共享同一个底层连接做并发操作。
- 若使用多线程 Web 服务，请使用“每请求独立 session / 连接”等模式，并理解池行为与单线程约束。

---

## 7. 运行方言测试套件（可选）

在已安装本包、已安装 `requirements_dev.txt`、且已配置好数据库的前提下，可参考 `setup.cfg` 中 `[db]` 与 `[sqla_testing]` 配置，使用 **pytest** 运行 `test/test_suite.py`。具体 profile 与连接串以你环境为准；勿将生产账号密码提交到版本库。

---

## 8. 已知能力边界（摘要）

详细说明见仓库内测试报告与 `yashandb_sqlalchemy/requirements.py` 中的 **SuiteRequirements** 声明。常见限制包括（随版本可能变化）：

- 部分与 **`UPDATE ... RETURNING`**、空字符串与 NULL 语义、极端标识符等相关的 SQLAlchemy 官方用例会被声明为不适用或跳过。
- 具体以当前代码与测试结论为准。

---

## 9. 相关文件索引

| 文件 | 作用 |
|------|------|
| `setup.py` | 包名、版本生成、`sqlalchemy.dialects` 入口注册 |
| `yashandb_sqlalchemy/yaspy.py` | yaspy 驱动方言实现 |
| `yashandb_sqlalchemy/base.py` | 公共编译器、类型、反射等基座 |
| `yashandb_sqlalchemy/requirements.py` | 方言测试能力声明 |
| `requirements_dev.txt` | 开发/测试依赖版本钉扎 |
| `test/test_suite.py` | SQLAlchemy 官方方言测试套件入口 |

如有问题，请结合厂商驱动文档与本仓库 issue/内部支持渠道反馈。
