import abc
import base64
import hashlib
import hmac
import json
import time
from typing import Dict, Optional
from urllib.parse import quote_plus

import aiohttp

from utils import async_retry_getter
from cachetools import TTLCache


class MessageSender:

    def __init__(self, cfg_dict: dict):
        self.secret = cfg_dict.get("secret", "")
        self.access_token = cfg_dict['access_token']
        self.cache = TTLCache(maxsize=100, ttl=cfg_dict.get('cache_ttl', 300))

    @abc.abstractmethod
    def __generate_post_url(self, session: aiohttp.ClientSession):
        raise NotImplementedError()

    @abc.abstractmethod
    async def send_message(self, msg, session: aiohttp.ClientSession):
        raise NotImplementedError()

    async def send_message_with_mark(self, msg, session: aiohttp.ClientSession, mark=None):
        if await self.check_cache(mark):
            return
        await self.send_message(msg, session)

    async def check_cache(self, mark):
        if mark in self.cache:
            self.cache[mark] = True
            return True
        else:
            self.cache[mark] = True
            return False


class DingDingSender(MessageSender):

    def __generate_post_url(self, session: aiohttp.ClientSession):
        secret = self.secret
        access_token = self.access_token
        timestamp = str(round(time.time() * 1000))
        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = quote_plus(base64.b64encode(hmac_code))
        url = f'https://oapi.dingtalk.com/robot/send?access_token={access_token}&timestamp={timestamp}&sign={sign}'
        return url

    async def send_message(self, msg, session: aiohttp.ClientSession):
        post_url = self.__generate_post_url(session)
        headers = {"Content-Type": "application/json", "Charset": "UTF-8"}
        req_json_str = json.dumps({"msgtype": "text", "text": {"content": msg}})
        await async_retry_getter(lambda: session.post(url=post_url, data=req_json_str, headers=headers))


class WechatSender(MessageSender):

    def __generate_post_url(self, session: aiohttp.ClientSession):
        url = f'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={self.access_token}'
        return url

    async def send_message(self, msg, session: aiohttp.ClientSession):
        post_url = self.__generate_post_url(session)
        headers = {"Content-Type": "application/json", "Charset": "UTF-8"}
        req_json_str = json.dumps({"msgtype": "text", "text": msg})
        await async_retry_getter(lambda: session.post(url=post_url, data=req_json_str, headers=headers))


class LarkSender(MessageSender):

    def __generate_post_url(self, session: aiohttp.ClientSession):
        url = f'https://open.feishu.cn/open-apis/bot/v2/hook/{self.access_token}'
        return url

    async def send_message(self, msg, session: aiohttp.ClientSession):
        post_url = self.__generate_post_url(session)
        headers = {"Content-Type": "application/json", "Charset": "UTF-8"}
        req_json_str = json.dumps({"msg_type": "text", "content": {"text": msg}})
        await async_retry_getter(lambda: session.post(url=post_url, data=req_json_str, headers=headers))


class MessageSenderFactory:
    @staticmethod
    def create_sender(cfg_dict) -> Optional[MessageSender]:
        if cfg_dict is None:
            return None
        else:
            platform = cfg_dict.get('platform')
            if platform == 'wechat':
                return WechatSender(cfg_dict)
            elif platform == 'dingding':
                return DingDingSender(cfg_dict)
            elif platform == 'feishu':
                return LarkSender(cfg_dict)
            else:
                raise ValueError(f"Unsupported platform: {platform}")

    @staticmethod
    def create_senders(cfg_dicts) -> Dict[str, MessageSender]:
        bot_dict = {}
        for cfg_dict in cfg_dicts:
            platform = cfg_dict.get('platform')
            if platform == 'wechat':
                bot_dict[cfg_dict['name']] = WechatSender(cfg_dict)
            elif platform == 'dingding':
                bot_dict[cfg_dict['name']] = DingDingSender(cfg_dict)
            elif platform == 'feishu':
                bot_dict[cfg_dict['name']] = LarkSender(cfg_dict)
            else:
                print(f"Unsupported platform: {platform}")
        return bot_dict
