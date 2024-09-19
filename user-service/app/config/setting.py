from starlette.config import Config
from starlette.datastructures import Secret

# ? Step-3: Create setting.py file for encrypting DatabaseURL

try:
    config = Config(".env")

except FileNotFoundError:
    config = Config()

DATABASE_URL = config("DATABASE_URL", cast=Secret)
TEST_DATABASE_URL = config("TEST_DATABASE_URL", cast=Secret)
SECRET_KEY = config("SECRET_KEY", cast=str)
ALGORITHYM = config("ALGORITHYM", cast=str)
EXPIRY_TIME = config("EXPIRY_TIME", cast=int)

BOOTSTRAP_SERVER1 = config("BOOTSTRAP_SERVER1", cast=str)
BOOTSTRAP_SERVER2 = config("BOOTSTRAP_SERVER2", cast=str)
BOOTSTRAP_SERVER3 = config("BOOTSTRAP_SERVER3", cast=str)

BOOTSTRAP_SERVERS = config("BOOTSTRAP_SERVERS", cast=str)

SALT = config("SALT", cast=str)
EMAIL_HOST = config("EMAIL_HOST", cast=str)
EMAIL_PORT = config("EMAIL_PORT", cast=int)
SENDER_EMAIL = config("SENDER_EMAIL", cast=str)
SENDER_PASSWORD = config("SENDER_PASSWORD", cast=str)
EMAIL_FROM = config("EMAIL_FROM", cast=str)

DOMAIN_NAME = config("DOMAIN_NAME", cast=str)
SERVICE = config("SERVICE", cast=str)
PREFIX = config("PREFIX", cast=str)


KAFKA_USER_REGISTER_TOPIC = config("KAFKA_USER_REGISTER_TOPIC", cast=str)
KAFKA_CONSUMER_GROUP_ID_FOR_USER_SERVICE = config("KAFKA_CONSUMER_GROUP_ID_FOR_USER_SERVICE", cast=str)