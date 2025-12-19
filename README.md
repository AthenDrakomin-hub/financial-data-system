# 金融数据系统

一个基于Python、Flask和Elasticsearch的金融数据抓取与分析系统。

## 功能特性

- 自动抓取新浪财经、东方财富网等财经网站数据
- 实时分析市场动态和交易策略
- 定时任务调度
- RESTful API接口
- 数据存储于Elasticsearch

## 技术栈

- **后端**: Python 3.7+, Flask 3.0
- **数据存储**: Elasticsearch 8.14.0
- **定时任务**: APScheduler 3.10.4
- **网页解析**: BeautifulSoup4, lxml
- **HTTP请求**: requests 2.31.0

## 快速开始

### 本地开发环境

1. 克隆仓库:
   ```bash
   git clone https://github.com/AthenDrakomin-hub/financial-data-system.git
   cd financial-data-system
   ```

2. 安装依赖:
   ```bash
   pip install -r requirements.txt
   ```

3. 启动Elasticsearch (使用Docker):
   ```bash
   docker run -d --name elasticsearch -p 9200:9200 -p 9300:9300 \
     -e "discovery.type=single-node" \
     -e "xpack.security.enabled=false" \
     docker.elastic.co/elasticsearch/elasticsearch:8.14.0
   ```

4. 启动系统:
   ```bash
   python finance_data_system_elastic.py
   ```

### 生产环境部署

1. 使用生产级WSGI服务器:
   ```bash
   pip install waitress
   python start_production.py
   ```

> 注意：在Windows系统上，可以使用`start_production.bat`脚本启动生产环境。该脚本仅在本地开发环境中提供，不会包含在代码仓库中。

## API接口

- `GET /api/v1/health` - 健康检查
- `GET /api/v1/data/pre_market` - 获取盘前策略
- `POST /api/v1/crawl/now` - 立即执行数据抓取
- `POST /api/v1/tasks/execute/<task_name>` - 执行指定任务

## 许可证

MIT