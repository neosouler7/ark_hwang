import telegram
import datetime

from utils.common import read_config


class TgManager:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(TgManager, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        config = read_config()
        self.bot = telegram.Bot(token=config.get('tg_bot_token'))
        self.chat_ids = config.get('tg_bot_chat_ids')

    def send_message(self, tg_msg):
        for u in self.bot.getUpdates():
            print(u)

        tg_msg += f'\n\n{datetime.datetime.now()}'
        for c in self.chat_ids:
            self.bot.send_message(c, tg_msg, timeout=30)
