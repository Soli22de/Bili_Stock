import requests
import json
import time
import random
import os
import pandas as pd
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/xueqiu_spy.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class XueqiuSpy:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://xueqiu.com/",
            "Origin": "https://xueqiu.com",
            "Host": "xueqiu.com",
            "X-Requested-With": "XMLHttpRequest"
        }
        self.targets_file = "data/active_target_cubes.json"
        self.output_file = "data/cube_rebalancing.csv"
        
        # 内存缓存：{cube_symbol: last_rebalance_id}
        # 实际生产中应持久化存储 (如 SQLite/Redis)
        self.last_rebalance_ids = {} 
        
        self._init_cookie()

    def _init_cookie(self):
        """访问主页获取初始 Cookie"""
        try:
            logging.info("正在获取雪球 Cookie...")
            # 手动注入用户提供的完整 Cookie
            raw_cookie = "acw_tc=3ccdc17e17713034273231509ee15c6e0f10dfb94b43e659b594d9d295e9b5; cookiesu=341771303427330; device_id=c803c3ee03e54a3a75ffde5e3f9b928d; Hm_lvt_1db88642e346389874251b5a1eded6e3=1771303428; HMACCOUNT=10B820C8E54C37A9; smidV2=20260217124348f9f63420da6530e44bccf87b1221265d009b6bb43cc4d88d0; xq_a_token=33c6a88412590f9d707de51fbca5a323ef9a0ef3; xqat=33c6a88412590f9d707de51fbca5a323ef9a0ef3; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjYyOTc4MjIyMzgsImlzcyI6InVjIiwiZXhwIjoxNzczODk1Mjg0LCJjdG0iOjE3NzEzMDM0Mzc5NDEsImNpZCI6ImQ5ZDBuNEFadXAifQ.QjcXIPhbZmzCzhl1h8WQDjPFWOwu1P70rITs1UO_JrulikDYlGAevgkLGj4bG1AeQK4P8OKoQHkVzZc_Y1C5mLYxIdtyGUwVmyWhOrvtBYpx-IdWDhfxelt9sUCeyzWPKMQGU6K9dX64b4PfJ2RU1AjkysRXdBaP_lwtIUygOFH_M0GatP31lfX-yVNS5HdhQx7GGZX2QHIOo5JYzV9Fk-kcUW_G17DOqqhA03ZFcfrtYiydjICQPD7pAiaXGWuV4h1dmkk--IMYIL2ihbGMzkiEiAMKvOedAw4yPvPJGu_yMauYC-KLV_E49UlWLOjR_F5X1z4Ey8xVEPst2XEXsw; xq_r_token=075e5a5288f1d196eed9ffa5cc99aca5e136bff8; xq_is_login=1; u=6297822238; is_overseas=0; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1771303439; .thumbcache_f24b8bbe5a5934237bbc0eda20c1b6e7=FHDhoCv08W8qq7LCvhI4wVC5fmdISE0i9YLaxXcdid1A+jiQmDbfysCKpXxIiCAi+YjiH73oKrrpKKQWDDHKEQ%3D%3D; ssxmod_itna=1-YqGO7KY5AIejOjDhx_oxKupDp2bb4DXDUkqiQGgDYq7=GFKDCgYIRr=m4BK1fq_qDAgDXGW46Y7tDlrOrYDSxD=HDK4GThCPeDt_jEw04vW8tiH_w9YiAoM9WYeC2bU53yKUq9UgXU=qqjWzhQiYDCPDExGk57A=hDiiHx0rD0eDPxDYDG4Do6YDn6xDjxDd84SAmRoDbxi3E4GCo__L24DFkAopR3xD0oa_HGbveDDzXovqrSijDiW_RU2Phz0uWihID753Dlc4zTGIV/1BeGSuHvZLdroDXZtDvrSUGzrQ1f8EXK_wdgAiY_x0PmnmM0peeeGBDpihhee5AwMAqpjG4jD2j51mqx9rrDDA4hqt_YtiYQ_q4xlIHZbc_0HY37eFR4EuxF_KREVn2D9u4CDHiwVRxNlxi4biB2zib4zGbwGhK74eD; ssxmod_itna2=1-YqGO7KY5AIejOjDhx_oxKupDp2bb4DXDUkqiQGgDYq7=GFKDCgYIRr=m4BK1fq_qDAgDXGW46Y7YDiPbH_C=bD7pGeDGunvDBw0ne5IICLLLS4MS5auxyU7fwE9KjpRc0/mTCk_GT1gvOkPvdWQ0HdW5xSlHi587_QIFatncWzOnxmP5IeiKw7zPOOgcxna9sWQcxWiI=EU8kEE8E84uhPUf6WA8U5jdh53eOXxwFn=iXgE0FxpahW=jqNNG08zDhWl9Xka0sO26h=KkuVOZGITUmh21cwyUm4y9t0R6HgfT_BzAq6hIfVAkEuYcEz62NKwkWohuMz/oCYm18_FRhDz4KXgha4i/gid7Hxxhvqazg36h3p3x1M=Dnsb4GnhWLbpqGr5RmmxNNCH53OmLif8DGuFzurVl5L8k9RWTODW23UbvLbWUBwT2mW_5USb0EaWQO_iehGDdGj6OrM24dYe57jU66MA5ViYxh5tYjUPXeiDD"
            
            # 将 raw cookie 解析到 session 中
            for cookie in raw_cookie.split('; '):
                if '=' in cookie:
                    key, value = cookie.split('=', 1)
                    self.session.cookies.set(key, value)
            
            self.session.headers.update(self.headers)
            
            # 验证一下
            if "xq_a_token" in self.session.cookies:
                logging.info(f"Cookie 注入成功: xq_a_token found")
            else:
                logging.warning(f"Cookie 可能不完整: {self.session.cookies.get_dict()}")
        except Exception as e:
            logging.error(f"Cookie 获取失败: {e}")

    def load_targets(self):
        """加载监控目标"""
        if not os.path.exists(self.targets_file):
            logging.error(f"目标文件 {self.targets_file} 不存在，请先运行 Hunter")
            return []
            
        try:
            with open(self.targets_file, 'r', encoding='utf-8') as f:
                targets = json.load(f)
                logging.info(f"加载了 {len(targets)} 个监控目标")
                return targets
        except Exception as e:
            logging.error(f"加载目标失败: {e}")
            return []

    def get_latest_moves(self, cube_symbol, count=20):
        """获取最新调仓动作"""
        # 即使注入了 Cookie，rebalancing/history 接口仍然可能校验 Referer 或其他头
        # 尝试直接调用 history 接口，而不是 HTML 解析
        
        url = "https://xueqiu.com/cubes/rebalancing/history.json"
        params = {
            "cube_symbol": cube_symbol,
            "count": count,
            "page": 1,
            "_": int(time.time() * 1000)
        }
        
        # 必须带上 Referer
        self.session.headers.update({
            "Referer": f"https://xueqiu.com/P/{cube_symbol}",
            "Host": "xueqiu.com"
        })
        
        try:
            resp = self.session.get(url, params=params)
            
            if resp.status_code != 200:
                # logging.warning(f"[{cube_symbol}] API请求失败: {resp.status_code}")
                # 如果 API 失败，再退回到 HTML 解析
                pass
            else:
                data = resp.json()
                moves = []
                if "list" in data:
                    for item in reversed(data["list"]):
                        rb_id = item["id"]
                        if rb_id > self.last_rebalance_ids.get(cube_symbol, 0):
                            if item["status"] == "success":
                                parsed_moves = self._parse_move(item)
                                if parsed_moves:
                                    moves.extend(parsed_moves)
                            self.last_rebalance_ids[cube_symbol] = rb_id
                    
                    if moves:
                        return moves
                    # 如果 API 返回空列表，也可能是权限问题，尝试 HTML 解析作为兜底
        except Exception as e:
            logging.error(f"API Error: {e}")

        # HTML 解析兜底 (之前的逻辑)
        # ... (保留之前的 HTML 解析代码，作为 fallback)
        
        url_html = f"https://xueqiu.com/P/{cube_symbol}"
        try:
            resp = self.session.get(url_html)
            html = resp.text
            
            # 模式4：直接匹配 cube_info 数据块
            import re
            match_cube = re.search(r'SNOWBALL_TARGET_CUBE\s*=\s*(\{.*?\});', html, re.DOTALL)
            
            if match_cube:
                try:
                    cube_data = json.loads(match_cube.group(1))
                    view_rebalancing = cube_data.get("view_rebalancing", {})
                    
                    if not view_rebalancing:
                        view_rebalancing = cube_data.get("last_rebalancing", {})
                        
                    if view_rebalancing and "id" in view_rebalancing:
                         latest_rebal = {"list": [view_rebalancing]}
                         data = latest_rebal
                         # logging.info(f"[{cube_symbol}] 从 TARGET_CUBE 提取到最近一次调仓")
                         
                         moves = []
                         if "list" in data:
                            for item in reversed(data["list"]):
                                rb_id = item.get("id", 0)
                                if rb_id > self.last_rebalance_ids.get(cube_symbol, 0):
                                    if item.get("status") == "success":
                                        parsed_moves = self._parse_move(item)
                                        if parsed_moves:
                                            moves.extend(parsed_moves)
                                    self.last_rebalance_ids[cube_symbol] = rb_id
                         return moves
                except:
                    pass
            
            # 如果还不行，打印一小段 HTML 看看是不是被重定向到了登录页
            if "登录" in html or "login" in html:
                 logging.error(f"[{cube_symbol}] Cookie 似乎失效，被重定向到登录页")
            else:
                 logging.warning(f"[{cube_symbol}] 未找到调仓数据")

            return []
        except:
            return []

    def _parse_move(self, item):
        """解析单次调仓记录"""
        signals = []
        updated_at = item["updated_at"] # 时间戳
        time_str = datetime.fromtimestamp(updated_at/1000).strftime('%Y-%m-%d %H:%M:%S')
        
        # 记录组合代码
        cube_symbol = item.get("cube_symbol", "")
        
        for hist in item.get("rebalancing_histories", []):
            stock_code = hist.get("stock_symbol")
            stock_name = hist.get("stock_name")
            prev_w = hist.get("prev_weight_adjusted", 0)
            target_w = hist.get("target_weight", 0)
            price = hist.get("price")
            
            if prev_w is None: prev_w = 0
            if target_w is None: target_w = 0
            
            # 计算仓位变化 delta
            delta = target_w - prev_w
            
            # 过滤微小调仓 (<0.5%)
            if abs(delta) < 0.5: 
                continue
                
            action = "BUY" if delta > 0 else "SELL"
            
            signals.append({
                "time": time_str,
                "cube_symbol": cube_symbol,
                "stock_code": stock_code,
                "stock_name": stock_name,
                "action": action,
                "delta": round(delta, 2), # 仓位变化
                "price": price,
                "comment": item.get("comment", "") or ""
            })
            
        return signals

    def save_signals(self, signals):
        """保存信号到 CSV"""
        if not signals:
            return

        df = pd.DataFrame(signals)
        
        # 如果文件不存在，写入表头；否则追加
        if not os.path.exists(self.output_file):
            df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
        else:
            df.to_csv(self.output_file, mode='a', header=False, index=False, encoding='utf-8-sig')
            
        logging.info(f"已保存 {len(signals)} 条新信号")
        print(df.to_string())

    def run_once(self):
        """执行一次全量轮询"""
        targets = self.load_targets()
        if not targets:
            return

        all_new_signals = []
        
        for target in targets:
            symbol = target['symbol']
            name = target['name']
            logging.info(f"正在监控: {name} ({symbol})...")
            
            moves = self.get_latest_moves(symbol)
            if moves:
                logging.info(f"[{name}] 发现 {len(moves)} 条新调仓!")
                all_new_signals.extend(moves)
            
            time.sleep(random.uniform(1, 3)) # 防风控
            
        self.save_signals(all_new_signals)

if __name__ == "__main__":
    spy = XueqiuSpy()
    # 首次运行会抓取最近的记录作为基准
    spy.run_once()
