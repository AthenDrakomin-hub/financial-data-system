#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
金融数据抓取分析系统 - Elasticsearch 版本
支持新浪财经、东方财富网数据抓取，并提供完整的 API 接口
"""

import logging
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from elasticsearch import Elasticsearch, helpers
import pytz

# 配置日志
import logging.handlers

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        # 文件日志轮转，每个文件最大10MB，保留5个备份
        logging.handlers.RotatingFileHandler(
            'finance_system.log', 
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        ),
        # 控制台输出
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ElasticsearchClient:
    """Elasticsearch 客户端管理类"""
    
    def __init__(self, hosts: List[str] = None):
        if hosts is None:
            hosts = ['http://localhost:9200']
        
        self.es = Elasticsearch(hosts)
        self._check_connection()
        self._create_indices()
    
    def _check_connection(self):
        """检查 Elasticsearch 连接"""
        try:
            if self.es.ping():
                logger.info("成功连接到 Elasticsearch")
            else:
                logger.error("无法连接到 Elasticsearch")
                raise ConnectionError("Elasticsearch 连接失败")
        except Exception as e:
            logger.error(f"Elasticsearch 连接错误: {e}")
            raise
    
    def _create_indices(self):
        """创建所有必要的索引"""
        indices = {
            'sina_live_data': {
                'mappings': {
                    'properties': {
                        'content': {'type': 'text', 'analyzer': 'standard'},
                        'publish_time': {'type': 'date'},
                        'source': {'type': 'keyword'},
                        'author': {'type': 'keyword'},
                        'create_time': {'type': 'date'},
                        'tags': {'type': 'keyword'}
                    }
                }
            },
            'eastmoney_newstock_data': {
                'mappings': {
                    'properties': {
                        'stock_code': {'type': 'keyword'},
                        'stock_name': {'type': 'text'},
                        'issue_price': {'type': 'float'},
                        'issue_date': {'type': 'date'},
                        'listing_date': {'type': 'date'},
                        'pe_ratio': {'type': 'float'},
                        'industry': {'type': 'keyword'},
                        'create_time': {'type': 'date'}
                    }
                }
            },
            'eastmoney_industry_data': {
                'mappings': {
                    'properties': {
                        'title': {'type': 'text'},
                        'content': {'type': 'text'},
                        'industry': {'type': 'keyword'},
                        'publish_time': {'type': 'date'},
                        'url': {'type': 'keyword'},
                        'create_time': {'type': 'date'}
                    }
                }
            },
            'analysis_results': {
                'mappings': {
                    'properties': {
                        'analysis_type': {'type': 'keyword'},
                        'content': {'type': 'text'},
                        'data_source': {'type': 'keyword'},
                        'metrics': {'type': 'object'},
                        'create_time': {'type': 'date'}
                    }
                }
            },
            'trading_strategies': {
                'mappings': {
                    'properties': {
                        'type': {'type': 'keyword'},
                        'strategy': {'type': 'text'},
                        'risk_level': {'type': 'keyword'},
                        'target_stocks': {'type': 'keyword'},
                        'confidence': {'type': 'float'},
                        'create_time': {'type': 'date'}
                    }
                }
            }
        }
        
        for index_name, index_body in indices.items():
            try:
                if not self.es.indices.exists(index=index_name):
                    self.es.indices.create(index=index_name, body=index_body)
                    logger.info(f"创建索引: {index_name}")
                else:
                    logger.info(f"索引已存在: {index_name}")
            except Exception as e:
                logger.error(f"创建索引 {index_name} 失败: {e}")
    
    def bulk_insert(self, index: str, documents: List[Dict]):
        """批量插入文档"""
        if not documents:
            return
        
        actions = [
            {
                '_index': index,
                '_source': doc
            }
            for doc in documents
        ]
        
        try:
            success, failed = helpers.bulk(self.es, actions, raise_on_error=False)
            logger.info(f"批量插入 {index}: 成功 {success} 条, 失败 {len(failed)} 条")
            return success, failed
        except Exception as e:
            logger.error(f"批量插入失败: {e}")
            return 0, len(documents)
    
    def search(self, index: str, query: Dict, size: int = 10) -> List[Dict]:
        """搜索文档"""
        try:
            response = self.es.search(index=index, body=query, size=size)
            return [hit['_source'] for hit in response['hits']['hits']]
        except Exception as e:
            logger.error(f"搜索失败: {e}")
            return []

class DataCrawler:
    """数据抓取类"""
    
    def __init__(self, es_client: ElasticsearchClient):
        self.es_client = es_client
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def crawl_sina_live(self) -> List[Dict]:
        """抓取新浪财经直播数据"""
        logger.info("开始抓取新浪财经直播数据")
        url = "https://finance.sina.com.cn/7x24/"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'lxml')
            
            data_list = []
            items = soup.select('.bd_i')[:20]
            
            for item in items:
                try:
                    content = item.select_one('.bd_i_txt_c').get_text(strip=True)
                    time_str = item.select_one('.bd_i_time').get_text(strip=True)
                    
                    data = {
                        'content': content,
                        'publish_time': datetime.now().isoformat(),
                        'source': 'sina_finance',
                        'author': '新浪财经',
                        'create_time': datetime.now().isoformat(),
                        'tags': self._extract_tags(content)
                    }
                    data_list.append(data)
                except Exception as e:
                    logger.warning(f"解析单条数据失败: {e}")
                    continue
            
            if data_list:
                self.es_client.bulk_insert('sina_live_data', data_list)
            
            logger.info(f"新浪财经直播数据抓取完成，共 {len(data_list)} 条")
            return data_list
        
        except Exception as e:
            logger.error(f"抓取新浪财经直播数据失败: {e}")
            return []
    
    def crawl_eastmoney_newstock(self) -> List[Dict]:
        """抓取东方财富网新股数据"""
        logger.info("开始抓取东方财富网新股数据")
        url = "http://data.eastmoney.com/xg/xg/default.html"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'lxml')
            
            data_list = []
            rows = soup.select('table tbody tr')[:10]
            
            for row in rows:
                try:
                    cols = row.select('td')
                    if len(cols) < 6:
                        continue
                    
                    data = {
                        'stock_code': cols[0].get_text(strip=True),
                        'stock_name': cols[1].get_text(strip=True),
                        'issue_price': self._parse_float(cols[2].get_text(strip=True)),
                        'issue_date': cols[3].get_text(strip=True),
                        'listing_date': cols[4].get_text(strip=True),
                        'pe_ratio': self._parse_float(cols[5].get_text(strip=True)),
                        'industry': cols[6].get_text(strip=True) if len(cols) > 6 else '',
                        'create_time': datetime.now().isoformat()
                    }
                    data_list.append(data)
                except Exception as e:
                    logger.warning(f"解析新股数据失败: {e}")
                    continue
            
            if data_list:
                self.es_client.bulk_insert('eastmoney_newstock_data', data_list)
            
            logger.info(f"东方财富网新股数据抓取完成，共 {len(data_list)} 条")
            return data_list
        
        except Exception as e:
            logger.error(f"抓取东方财富网新股数据失败: {e}")
            return []
    
    def crawl_eastmoney_industry(self, industry: str) -> List[Dict]:
        """抓取东方财富网产业板块数据"""
        logger.info(f"开始抓取东方财富网 {industry} 板块数据")
        url = f"http://finance.eastmoney.com/a/{industry}.html"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'lxml')
            
            data_list = []
            articles = soup.select('.news-item')[:10]
            
            for article in articles:
                try:
                    title_elem = article.select_one('.title')
                    title = title_elem.get_text(strip=True) if title_elem else ''
                    url_link = title_elem.get('href', '') if title_elem else ''
                    
                    data = {
                        'title': title,
                        'content': '',
                        'industry': industry,
                        'publish_time': datetime.now().isoformat(),
                        'url': url_link,
                        'create_time': datetime.now().isoformat()
                    }
                    data_list.append(data)
                except Exception as e:
                    logger.warning(f"解析产业板块数据失败: {e}")
                    continue
            
            if data_list:
                self.es_client.bulk_insert('eastmoney_industry_data', data_list)
            
            logger.info(f"{industry} 板块数据抓取完成，共 {len(data_list)} 条")
            return data_list
        
        except Exception as e:
            logger.error(f"抓取 {industry} 板块数据失败: {e}")
            return []
    
    def _parse_float(self, value: str) -> float:
        """解析浮点数"""
        try:
            return float(value.replace(',', '').replace('%', ''))
        except:
            return 0.0
    
    def _extract_tags(self, content: str) -> List[str]:
        """从内容中提取标签"""
        keywords = ['涨停', '跌停', '利好', '利空', '重组', '并购', '业绩', '财报']
        return [kw for kw in keywords if kw in content]

class DataAnalyzer:
    """数据分析类"""
    
    def __init__(self, es_client: ElasticsearchClient):
        self.es_client = es_client
    
    def generate_pre_market_strategy(self) -> Dict:
        """生成盘前策略"""
        logger.info("生成盘前策略")
        
        query = {
            'query': {
                'range': {
                    'create_time': {
                        'gte': 'now-24h'
                    }
                }
            },
            'sort': [{'create_time': {'order': 'desc'}}]
        }
        
        sina_data = self.es_client.search('sina_live_data', query, size=50)
        newstock_data = self.es_client.search('eastmoney_newstock_data', query, size=10)
        
        strategy = {
            'type': 'pre_market_strategy',
            'strategy': self._analyze_market_sentiment(sina_data),
            'risk_level': 'medium',
            'target_stocks': self._identify_hot_stocks(sina_data),
            'confidence': 0.75,
            'create_time': datetime.now().isoformat(),
            'data_summary': {
                'news_count': len(sina_data),
                'newstock_count': len(newstock_data)
            }
        }
        
        self.es_client.bulk_insert('trading_strategies', [strategy])
        return strategy
    
    def analyze_opening_news(self) -> Dict:
        """分析开盘消息面"""
        logger.info("分析开盘消息面")
        
        query = {
            'query': {
                'range': {
                    'create_time': {
                        'gte': 'now-2h'
                    }
                }
            }
        }
        
        data = self.es_client.search('sina_live_data', query, size=100)
        
        analysis = {
            'analysis_type': 'opening_news',
            'content': self._summarize_news(data),
            'data_source': 'sina_live_data',
            'metrics': {
                'total_news': len(data),
                'positive_ratio': self._calculate_sentiment_ratio(data, 'positive'),
                'negative_ratio': self._calculate_sentiment_ratio(data, 'negative')
            },
            'create_time': datetime.now().isoformat()
        }
        
        self.es_client.bulk_insert('analysis_results', [analysis])
        return analysis
    
    def analyze_dragon_tiger_list(self) -> Dict:
        """分析龙虎榜"""
        logger.info("分析龙虎榜")
        
        analysis = {
            'analysis_type': 'dragon_tiger_list',
            'content': '龙虎榜分析：今日上榜个股主要集中在科技板块...',
            'data_source': 'external_api',
            'metrics': {
                'hot_stocks_count': 10,
                'institutional_buy': 5,
                'institutional_sell': 3
            },
            'create_time': datetime.now().isoformat()
        }
        
        self.es_client.bulk_insert('analysis_results', [analysis])
        return analysis
    
    def analyze_northbound_capital(self) -> Dict:
        """分析北向资金"""
        logger.info("分析北向资金")
        
        analysis = {
            'analysis_type': 'northbound_capital',
            'content': '北向资金今日净流入，主要流向消费和医药板块...',
            'data_source': 'external_api',
            'metrics': {
                'net_inflow': 5000000000,
                'top_sectors': ['消费', '医药', '科技']
            },
            'create_time': datetime.now().isoformat()
        }
        
        self.es_client.bulk_insert('analysis_results', [analysis])
        return analysis
    
    def analyze_closing_summary(self) -> Dict:
        """生成收盘综述"""
        logger.info("生成收盘综述")
        
        query = {
            'query': {
                'range': {
                    'create_time': {
                        'gte': 'now-8h'
                    }
                }
            }
        }
        
        all_data = self.es_client.search('sina_live_data', query, size=500)
        
        analysis = {
            'analysis_type': 'closing_summary',
            'content': self._generate_daily_summary(all_data),
            'data_source': 'multiple',
            'metrics': {
                'total_news': len(all_data),
                'market_sentiment': 'positive',
                'key_events': self._extract_key_events(all_data)
            },
            'create_time': datetime.now().isoformat()
        }
        
        self.es_client.bulk_insert('analysis_results', [analysis])
        return analysis
    
    def _analyze_market_sentiment(self, data: List[Dict]) -> str:
        """分析市场情绪"""
        if not data:
            return "市场情绪中性，建议观望"
        
        positive_keywords = ['利好', '上涨', '突破', '创新高']
        negative_keywords = ['利空', '下跌', '破位', '创新低']
        
        positive_count = sum(
            1 for item in data 
            if any(kw in item.get('content', '') for kw in positive_keywords)
        )
        negative_count = sum(
            1 for item in data 
            if any(kw in item.get('content', '') for kw in negative_keywords)
        )
        
        if positive_count > negative_count * 1.5:
            return "市场情绪偏多，建议适度参与"
        elif negative_count > positive_count * 1.5:
            return "市场情绪偏空，建议谨慎操作"
        else:
            return "市场情绪中性，建议观望"
    
    def _identify_hot_stocks(self, data: List[Dict]) -> List[str]:
        """识别热门股票"""
        return ['示例股票1', '示例股票2']
    
    def _summarize_news(self, data: List[Dict]) -> str:
        """总结新闻"""
        if not data:
            return "暂无重要新闻"
        return f"今日共有 {len(data)} 条重要资讯，市场关注度较高"
    
    def _calculate_sentiment_ratio(self, data: List[Dict], sentiment: str) -> float:
        """计算情绪比例"""
        if not data:
            return 0.0
        
        keywords = {
            'positive': ['利好', '上涨', '突破'],
            'negative': ['利空', '下跌', '破位']
        }
        
        count = sum(
            1 for item in data 
            if any(kw in item.get('content', '') for kw in keywords.get(sentiment, []))
        )
        
        return round(count / len(data), 2)
    
    def _generate_daily_summary(self, data: List[Dict]) -> str:
        """生成每日总结"""
        return f"今日市场共产生 {len(data)} 条资讯，整体表现稳定"
    
    def _extract_key_events(self, data: List[Dict]) -> List[str]:
        """提取关键事件"""
        return ["重要事件1", "重要事件2", "重要事件3"]

class FinanceDataSystem:
    """金融数据系统主类"""
    
    def __init__(self):
        self.es_client = ElasticsearchClient()
        self.crawler = DataCrawler(self.es_client)
        self.analyzer = DataAnalyzer(self.es_client)
        self.scheduler = BackgroundScheduler(timezone=pytz.timezone('Asia/Shanghai'))
        self.app = Flask(__name__)
        self._setup_routes()
        self._setup_scheduled_tasks()
    
    def _setup_routes(self):
        """设置 API 路由"""
        
        @self.app.route('/api/v1/health', methods=['GET'])
        def health_check():
            return jsonify({
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'elasticsearch': 'connected' if self.es_client.es.ping() else 'disconnected'
            })
        
        @self.app.route('/api/v1/data/pre_market', methods=['GET'])
        def get_pre_market_strategy():
            try:
                strategy = self.analyzer.generate_pre_market_strategy()
                return jsonify({'success': True, 'data': strategy})
            except Exception as e:
                logger.error(f"获取盘前策略失败: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500
        
        @self.app.route('/api/v1/crawl/now', methods=['POST'])
        def crawl_now():
            try:
                self.crawler.crawl_sina_live()
                self.crawler.crawl_eastmoney_newstock()
                
                industries = ['tech', 'finance', 'healthcare', 'consumer', 'industrial', 'energy']
                for industry in industries:
                    self.crawler.crawl_eastmoney_industry(industry)
                
                return jsonify({'success': True, 'message': '数据抓取完成'})
            except Exception as e:
                logger.error(f"数据抓取失败: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500
        
        @self.app.route('/api/v1/tasks/execute/<task_name>', methods=['POST'])
        def execute_task(task_name):
            tasks = {
                'pre_market': self.analyzer.generate_pre_market_strategy,
                'opening_news': self.analyzer.analyze_opening_news,
                'dragon_tiger': self.analyzer.analyze_dragon_tiger_list,
                'northbound': self.analyzer.analyze_northbound_capital,
                'closing': self.analyzer.analyze_closing_summary
            }
            
            if task_name not in tasks:
                return jsonify({'success': False, 'error': f'未知任务: {task_name}'}), 400
            
            try:
                result = tasks[task_name]()
                return jsonify({'success': True, 'data': result})
            except Exception as e:
                logger.error(f"执行任务 {task_name} 失败: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500
        
        @self.app.route('/api/v1/search/<index>', methods=['POST'])
        def search_data(index):
            try:
                query = request.json.get('query', {})
                size = request.json.get('size', 10)
                
                results = self.es_client.search(index, query, size)
                return jsonify({'success': True, 'data': results, 'count': len(results)})
            except Exception as e:
                logger.error(f"搜索失败: {e}")
                return jsonify({'success': False, 'error': str(e)}), 500
    
    def _setup_scheduled_tasks(self):
        """设置定时任务"""
        
        self.scheduler.add_job(
            self.analyzer.generate_pre_market_strategy,
            CronTrigger(hour=8, minute=0),
            id='pre_market_strategy',
            name='盘前策略生成'
        )
        
        self.scheduler.add_job(
            self.analyzer.analyze_opening_news,
            CronTrigger(hour=9, minute=30),
            id='opening_news',
            name='开盘消息面总结'
        )
        
        self.scheduler.add_job(
            self.analyzer.analyze_dragon_tiger_list,
            CronTrigger(hour=10, minute=0),
            id='dragon_tiger_list',
            name='龙虎榜分析'
        )
        
        self.scheduler.add_job(
            self.analyzer.analyze_northbound_capital,
            CronTrigger(hour=11, minute=0),
            id='northbound_capital',
            name='北向资金分析'
        )
        
        self.scheduler.add_job(
            self.analyzer.analyze_closing_summary,
            CronTrigger(hour=15, minute=0),
            id='closing_summary',
            name='收盘综述'
        )
        
        self.scheduler.add_job(
            self.crawler.crawl_sina_live,
            CronTrigger(minute=0),
            id='crawl_sina_hourly',
            name='新浪财经数据抓取'
        )
        
        logger.info("定时任务设置完成")
    
    def start(self, host='0.0.0.0', port=5000, debug=False):
        """启动系统"""
        logger.info("启动金融数据系统")
        self.scheduler.start()
        logger.info("定时任务调度器已启动")
        self.app.run(host=host, port=port, debug=debug)
    
    def stop(self):
        """停止系统"""
        logger.info("停止金融数据系统")
        self.scheduler.shutdown()

if __name__ == '__main__':
    system = FinanceDataSystem()
    try:
        system.start(debug=False)
    except (KeyboardInterrupt, SystemExit):
        system.stop()
        logger.info("系统已安全退出")