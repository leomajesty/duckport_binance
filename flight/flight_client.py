# -*- coding: utf-8 -*-
import json
import datetime

import pyarrow.flight as flight

from utils.log_kit import logger

class FlightClient:
    """Flight客户端，用于连接和查询Flight服务器"""
    
    def __init__(self, host: str = "localhost", port: int = 8815):
        self.location = f"grpc://{host}:{port}"
        self.client = flight.FlightClient(self.location)
        logger.info(f"连接到Flight服务器: {self.location}")
    
    def list_available_data(self):
        """列出可用的数据源"""
        try:
            flights = list(self.client.list_flights())
            logger.info(f"发现 {len(flights)} 个数据源:")
            
            for flight_info in flights:
                descriptor = flight_info.descriptor
                path = descriptor.path[0] if descriptor.path else "未知"
                total_records = flight_info.total_records
                logger.info(f"  - {path}: {total_records} 条记录")
            
            return flights
        except Exception as e:
            logger.error(f"获取数据源列表失败: {e}")
            return []
    
    def get_schema(self, table_name: str):
        """获取表的schema"""
        try:
            logger.info(f"请求获取表 '{table_name}' 的schema")
            descriptor = flight.FlightDescriptor.for_path(table_name)
            schema_result = self.client.get_schema(descriptor)
            schema = schema_result.schema
            logger.info(f"表 {table_name} 的schema:")
            for field in schema:
                logger.info(f"  - {field.name}: {field.type}")
            return schema
        except Exception as e:
            logger.error(f"获取表 '{table_name}' 的schema失败: {e}")
            return None
    
    def ping(self):
        """健康检查"""
        try:
            action = flight.Action("ping", b"")
            results = self.client.do_action(action)
            for result in results:
                response = result.body.to_pybytes().decode('utf-8')
                logger.info(f"服务器响应: {response}")
                return response
        except Exception as e:
            logger.error(f"健康检查失败: {e}")
            return None

    def ready(self, market: str = "usdt_perp"):
        """检查指定市场和时间间隔的数据是否准备好"""
        try:
            action = flight.Action("ready", json.dumps({"market": market}).encode())
            results = self.client.do_action(action)
            for result in results:
                response = result.body.to_pybytes().decode('utf-8')
                logger.info(f"{market}最近更新时间: {response}")
                return response
        except Exception as e:
            logger.error(f"检查数据准备状态失败: {e}")
            return None


    def list_actions(self):
        """列出所有可用的actions"""
        try:
            actions = list(self.client.list_actions())
            logger.info(f"发现 {len(actions)} 个可用的actions:")
            for action in actions:
                logger.info(f"  - {action.type}: {action.description}")
            return actions
        except Exception as e:
            logger.error(f"获取actions列表失败: {e}")
            return []

    def get_market(self, market: str, interval: int = 60, offset: int = 0,
                   begin: datetime.datetime = None, end: datetime.datetime = None):
        """执行SQL查询"""
        try:
            if not begin:
                begin = datetime.datetime.now() - datetime.timedelta(days=100)
            # 创建ticket
            ticket_dict = {"type": "market", "market": market, "interval": interval, "offset": offset,
                           "begin": begin.strftime('%Y-%m-%d %H:%M:%S')}
            if end:
                ticket_dict["end"] = end.strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"执行查询: {ticket_dict}")
            # dict转json str
            ticket = flight.Ticket(str(json.dumps(ticket_dict)).encode())

            # 获取数据
            reader = self.client.do_get(ticket)
            table = reader.read_all()

            logger.info(f"查询返回 {table.num_rows} 行数据")
            return table
        except Exception as e:
            logger.error(f"执行查询失败: {e}")
            return None

    def get_funding(self, market: str = "usdt_perp",
                    begin: datetime.datetime = None, end: datetime.datetime = None):
        """获取资金费率数据"""
        try:
            if not begin:
                begin = datetime.datetime.now() - datetime.timedelta(days=100)
            # 创建ticket
            ticket_dict = {"type": "funding", "market": market, "begin": begin.strftime('%Y-%m-%d %H:%M:%S')}
            if end:
                ticket_dict["end"] = end.strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"执行资金费率查询: {ticket_dict}")
            # dict转json str
            ticket = flight.Ticket(str(json.dumps(ticket_dict)).encode())

            # 获取数据
            reader = self.client.do_get(ticket)
            table = reader.read_all()

            logger.info(f"资金费率查询返回 {table.num_rows} 行数据")
            return table
        except Exception as e:
            logger.error(f"获取资金费率数据失败: {e}")
            return None

    def get_symbol(self, market: str, symbol: str, interval: int = 60, offset: int = 0,
                   begin: datetime.datetime = None, end: datetime.datetime = None):
        """获取指定symbol的数据"""
        try:
            if not begin:
                begin = datetime.datetime.now() - datetime.timedelta(days=100)
            # 创建ticket
            ticket_dict = {"type": "symbol", "market": market, "symbol": symbol,
                            "interval": interval, "offset": offset,
                           "begin": begin.strftime('%Y-%m-%d %H:%M:%S')}
            if end:
                ticket_dict["end"] = end.strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"执行symbol查询: {ticket_dict}")
            # dict转json str
            ticket = flight.Ticket(str(json.dumps(ticket_dict)).encode())

            # 获取数据
            reader = self.client.do_get(ticket)
            table = reader.read_all()

            logger.info(f"symbol查询返回 {table.num_rows} 行数据")
            return table
        except Exception as e:
            logger.error(f"获取symbol数据失败: {e}")
            return None

    def get_exginfo(self, market: str = "usdt_perp"):
        """获取交易所信息"""
        try:
            # 创建ticket
            ticket_dict = {"type": "exginfo", "market": market}
            logger.info(f"执行交易所信息查询: {ticket_dict}")
            # dict转json str
            ticket = flight.Ticket(str(json.dumps(ticket_dict)).encode())

            # 获取数据
            reader = self.client.do_get(ticket)
            table = reader.read_all()

            logger.info(f"交易所信息查询返回 {table.num_rows} 行数据")
            return table
        except Exception as e:
            logger.error(f"获取交易所信息失败: {e}")
            return None

    def close(self):
        """关闭客户端连接"""
        try:
            self.client.close()
            logger.info("Flight客户端连接已关闭")
        except Exception as e:
            logger.error(f"关闭Flight客户端失败: {e}")