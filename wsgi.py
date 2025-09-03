# wsgi.py  — production entrypoint for Render
# 1) run Nova boot (all schedulers, watchdogs, etc.)
# 2) expose Flask app object as `app` for gunicorn

from main import boot  # you’ll add "def boot()" in step 2
boot()

# your Telegram webhook Flask app lives in telegram_webhook.py
from telegram_webhook import telegram_app as app  # gunicorn looks for `app`
