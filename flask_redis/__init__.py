"""
Redis的Flask扩展
"""
from .redis_ext import RedisExtension
from .macro import K_RDS_BINDS


class Redis:

    def __init__(self, app=None, config=None):
        if not (config is None or isinstance(config, dict)):
            raise ValueError("`config` must be an instance of dict or None")

        self.config = config
        self.instances = dict()

        if app is not None:
            self.app = app
            self.init_app(app, config)

    def init_app(self, app, config=None):
        if not (config is None or isinstance(config, dict)):
            raise ValueError("`config` must be an install of dict or None")

        base_config = app.config.copy()
        if self.config:
            base_config.update(self.config)
        if config:
            base_config.update(config)
        self.config = base_config

        rds_binds = self.config.get(K_RDS_BINDS)
        if rds_binds and isinstance(rds_binds, dict):
            for key, cfg in self.config[K_RDS_BINDS].items():
                self.instances[key] = RedisExtension(cfg)
        else:
            self.instances["__rds__"] = RedisExtension(self.config)

        self.app = app
        app.extensions['flask_redis'] = self

    def __getattr__(self, item):
        return getattr(self.instances.get("__rds__"), item)

    def __getitem__(self, item):
        obj = self.instances.get(item or "__rds__")
        if obj is None:
            raise KeyError("Cannot read db with name '%s'" % item)
        return obj
