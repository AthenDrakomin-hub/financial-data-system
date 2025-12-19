#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
金融数据系统生产环境启动脚本
使用Waitress作为WSGI服务器替代Flask内置开发服务器
"""

import os
import sys
from waitress import serve
from finance_data_system_elastic import FinanceDataSystem

def main():
    """启动生产环境服务器"""
    # 从环境变量获取配置，如果不存在则使用默认值
    host = os.environ.get('FLASK_HOST', '0.0.0.0')
    port = int(os.environ.get('FLASK_PORT', 5000))
    threads = int(os.environ.get('WAITRESS_THREADS', 2))
    
    print(f"启动金融数据系统生产环境服务器...")
    print(f"监听地址: {host}:{port}")
    print(f"线程数: {threads}")
    
    # 初始化系统
    system = FinanceDataSystem()
    
    # 启动Waitress服务器
    try:
        # 内存优化配置
        serve(
            system.app, 
            host=host, 
            port=port, 
            threads=threads,
            connection_limit=100,
            backlog=2048,
            outbuf_overflow=16777216  # 16MB
        )
    except KeyboardInterrupt:
        print("正在关闭服务器...")
        system.stop()
        print("服务器已关闭")

if __name__ == '__main__':
    main()