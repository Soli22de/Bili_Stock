# 项目配置文件

# Gemini API Key (用于OCR和信号提取)
GEMINI_API_KEY = ""  # 请在此处填入您的 Gemini API Key

# DeepSeek API Key (用于市场情绪分析和策略生成)
DEEPSEEK_API_KEY = "sk-f7a77c7d640d40f089af2c57d2370e02" # 请填入您的 DeepSeek API Key
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEEPSEEK_MODEL = "deepseek-chat" # 或 deepseek-reasoner

# Database Configuration (Supabase PostgreSQL - Free Tier)
# 1. Sign up at https://supabase.com/
# 2. Create a new project -> Settings -> Database -> Connection string -> URI
# 3. Connection string format: "postgresql+psycopg2://postgres:[YOUR-PASSWORD]@db.xxxxxxxx.supabase.co:5432/postgres"
# Note: Requires `pip install psycopg2-binary`
DB_URL = None

# 钉钉机器人 Webhook (请替换为您自己的 Webhook 地址)
# 获取方式：钉钉群 -> 智能群助手 -> 添加机器人 -> 自定义 -> 复制 Webhook
DINGTALK_WEBHOOK = ""

# 钉钉机器人加签密钥 (可选，如果开启了加签安全设置)
DINGTALK_SECRET = ""
DINGTALK_KEYWORDS = ["葵花宝典"]

# Tushare Token (用于获取分钟级数据，强烈建议配置)
# 注册地址: https://tushare.pro/register
TUSHARE_TOKEN = ""
ENABLE_TUSHARE = True
ENABLE_TUSHARE_MINUTE = True
DISABLE_PROXY = True

# 股票代码映射表路径
STOCK_MAP_PATH = "data/stock_map_final.json"

# 数据集路径
VIDEOS_CSV = "data/dataset_videos.csv"
COMMENTS_CSV = "data/dataset_comments.csv"
SIGNALS_CSV = "data/trading_signals.csv"
BACKTEST_REPORT = "data/backtest_report.csv"

# 监控设置
MONITOR_INTERVAL = 300 # 监控轮询间隔 (秒)

# 实时盘中验证设置（BaoStock）
ENABLE_REALTIME_VALIDATION = True
MIN_VALIDATION_SCORE = 0.3
MAX_VALIDATION_SCORE = 2.0

# 技术指标设置（日线）
ENABLE_TECHNICAL_INDICATORS = True
INDICATOR_LOOKBACK_DAYS = 200
RSI_BUY_MAX = 35
RSI_SELL_MIN = 65

MORNING_SUMMARY_TIME = "09:20"
AUCTION_FILTER_TIME = "09:25"
CLOSE_SUMMARY_TIME = "15:05"
BUY_SIGNAL_MIN_SCORE = 0.90
REQUIRE_AUCTION_POOL = True
