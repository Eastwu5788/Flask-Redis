"""
Redis的Flask扩展
"""
from .redis_ext import RedisExtension
from .macro import K_RDS_BINDS, K_RDS_DEFAULT_BIND


class Redis:

    def __init__(self, app=None, config=None):
        """ Init Redis object from flask application and config

        :param app: Flask application
        :param config: configuration
        """
        self.config = config
        self.instances = dict()
        self.default_bind = "__rds__"

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

        # Init default property
        self.default_bind = self.config.pop(K_RDS_DEFAULT_BIND, "__rds__")

        # Init redis client instances
        rds_binds = self.config.pop(K_RDS_BINDS, None)
        if rds_binds and isinstance(rds_binds, dict):
            for key, cfg in rds_binds.items():
                self.instances[key] = RedisExtension(cfg)
        else:
            self.instances[self.default_bind] = RedisExtension(self.config)

        self.app = app
        app.extensions['flask_redis'] = self

    def __getattr__(self, item):
        obj = self.instances.get(self.default_bind)
        if obj is None:
            raise KeyError("Cannot load redis client")
        return getattr(obj, item)

    def __getitem__(self, item):
        """
        :rtype: RedisExtension
        """
        obj = self.instances.get(item or self.default_bind)
        if obj is None:
            raise KeyError("Cannot read db with name '%s'" % item)
        return obj

    def __delitem__(self, name):
        obj = self.instances.get(self.default_bind)
        if obj is None:
            raise KeyError("Cannot load redis client")
        obj.delete(name)
