# flask_redis
redis extension of flask

# Set Up
```
from flask imort Flask
from flask_redis import Redis

app = Flask(__name__)
redis = Redis(app, config={"CACHE_REDIS_HOST": "localhost"})
```
You may also set up your Redis instance later at configuration time using init_app method:
```
redis = Redis()

app = Flask(__name__)
redis.init_app(app)
```

# Configuring Flask-Redis
| Config Key | Introduction |
| :------   | :-------  |
|  CACHE_REDIS_HOST  |  A Redis server host.   |
|  CACHE_REDIS_PORT  |  A Redis server port. Default is 6379.   |
|  CACHE_REDIS_PASSWORD  |  A Redis password for server.  |
|  CACHE_REDIS_DB  |  A Redis db (zero-based number index). Default is 0.  |
