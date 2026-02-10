import aiohttp
import json
import logging
import time
import hmac
import hashlib
import base64
import urllib.parse
try:
    from config import DINGTALK_WEBHOOK, DINGTALK_SECRET
except ImportError:
    DINGTALK_WEBHOOK = ""
    DINGTALK_SECRET = ""

class DingTalkNotifier:
    def __init__(self, webhook=None, secret=None):
        self.webhook = webhook or DINGTALK_WEBHOOK
        self.secret = secret or DINGTALK_SECRET
        self.session = None

    async def init_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None

    def _sign(self):
        """
        生成加签后的 Webhook URL
        """
        if not self.secret:
            return self.webhook
        
        timestamp = str(round(time.time() * 1000))
        secret_enc = self.secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, self.secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return f"{self.webhook}&timestamp={timestamp}&sign={sign}"

    def _get_keywords(self):
        try:
            from config import DINGTALK_KEYWORDS
            return [k for k in DINGTALK_KEYWORDS if k]
        except Exception:
            return []

    def _append_keyword(self, text):
        keywords = self._get_keywords()
        missing = [k for k in keywords if k not in text]
        if missing:
            return f"{text}\n\n" + " ".join(missing)
        return text

    def _append_keyword_to_title(self, title):
        keywords = self._get_keywords()
        missing = [k for k in keywords if k not in title]
        if missing:
            return f"{title} " + " ".join(missing)
        return title

    async def send_text(self, content, at_mobiles=[]):
        """
        发送纯文本消息
        """
        if not self.webhook:
            logging.warning("DingTalk Webhook not configured. Skipping notification.")
            return False
        content = self._append_keyword(content)
        
        url = self._sign()
        headers = {'Content-Type': 'application/json'}
        data = {
            "msgtype": "text",
            "text": {
                "content": content
            },
            "at": {
                "atMobiles": at_mobiles,
                "isAtAll": False
            }
        }
        
        await self.init_session()
        try:
            async with self.session.post(url, json=data, headers=headers) as resp:
                if resp.status == 200:
                    res_json = await resp.json()
                    if res_json.get('errcode') == 0:
                        logging.info("DingTalk notification sent successfully.")
                        return True
                    else:
                        logging.error(f"DingTalk API Error: {res_json}")
                else:
                    logging.error(f"DingTalk HTTP Error: {resp.status}")
        except Exception as e:
            logging.error(f"Failed to send DingTalk notification: {e}")
        return False

    async def send_markdown(self, title, text, at_mobiles=[]):
        """
        发送 Markdown 消息
        """
        if not self.webhook:
            logging.warning("DingTalk Webhook not configured. Skipping notification.")
            return False
        text = self._append_keyword(text)
        title = self._append_keyword_to_title(title)
            
        url = self._sign()
        headers = {'Content-Type': 'application/json'}
        data = {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": text
            },
            "at": {
                "atMobiles": at_mobiles,
                "isAtAll": False
            }
        }
        
        await self.init_session()
        try:
            async with self.session.post(url, json=data, headers=headers) as resp:
                if resp.status == 200:
                    res_json = await resp.json()
                    if res_json.get('errcode') == 0:
                        logging.info("DingTalk notification sent successfully.")
                        return True
                    else:
                        logging.error(f"DingTalk API Error: {res_json}")
                else:
                    logging.error(f"DingTalk HTTP Error: {resp.status}")
        except Exception as e:
            logging.error(f"Failed to send DingTalk notification: {e}")
        return False

if __name__ == "__main__":
    # 测试代码
    import asyncio
    async def test():
        notifier = DingTalkNotifier()
        if not notifier.webhook:
            print("请先在 config.py 中配置 DINGTALK_WEBHOOK")
            return
        await notifier.send_markdown("测试标题", "# 这是一个测试\n- 列表项1\n- **加粗项**")
        await notifier.close_session()
    
    asyncio.run(test())
