import os
from dotenv import load_dotenv

load_dotenv()

# Binance API配置
BINANCE_CONFIG = {
    'BASE_URL': 'https://fapi.binance.com',
    'WS_URL': 'wss://fstream.binance.com/ws',
    'API_KEY': os.getenv('BINANCE_API_KEY'),
    'API_SECRET': os.getenv('BINANCE_API_SECRET')
}

# Redis配置
REDIS_CONFIG = {
    'HOST': os.getenv('REDIS_HOST', 'localhost'),
    'PORT': int(os.getenv('REDIS_PORT', 6379)),
    'DB': int(os.getenv('REDIS_DB', 0)),
    'PASSWORD': os.getenv('REDIS_PASSWORD'),
    'STREAMS': {
        'KLINE': 'market:kline',
        'ORDERBOOK': 'market:orderbook',
        'MAX_LENGTH': 10000,
        'CONSUMER_GROUP': 'market_data_processors'
    }
}

# MySQL配置
MYSQL_CONFIG = {
    'URL': f"mysql+aiomysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
          f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
}

# 同步配置
SYNC_CONFIG = {
    'KLINE': {
        'BATCH_SIZE': 1000,
        'MAX_CONCURRENT_REQUESTS': 20,
        'SYNC_INTERVAL': 300,  # 5分钟
        'MAX_LENGTH': 10000,   # Redis stream最大长度
        'CONSUMER_BATCH_SIZE': 100,  # 消费者批处理大小
        'PROCESSING_INTERVAL': 1,  # 处理间隔(秒)
    },
    'ORDERBOOK': {
        'DEPTH_LEVEL': 20,
        'UPDATE_SPEED': '100ms',
        'SNAPSHOT_INTERVAL': 3600,  # 1小时
        'MAX_LENGTH': 1000,
        'CONSUMER_BATCH_SIZE': 1000
    }
}

# 日志配置
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'INFO'
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': 'app.log',
            'formatter': 'standard',
            'level': 'INFO'
        }
    },
    'loggers': {
        '': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': True
        }
    }
} 