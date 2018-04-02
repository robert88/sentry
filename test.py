from sentry.utils.runner import configure; configure()

import random
from sentry.tsdb.base import BaseTSDB
from sentry.utils.services import LazyServiceWrapper


backend_path = 'sentry.utils.services.ServiceDelegator'
backend_opts = {
    'backend_base': 'sentry.tsdb.base.BaseTSDB',
    'backends': {
        'redis': {
            'path': 'sentry.tsdb.redis.RedisTSDB',
            'executor': {
                'path': 'sentry.utils.services.ThreadedExecutor',
                'options': {
                    'worker_count': 1,
                },
            },
        },
        'dummy': {
            'path': 'sentry.tsdb.dummy.DummyTSDB',
            'executor': {
                'path': 'sentry.utils.services.ThreadedExecutor',
                'options': {
                    'worker_count': 4,
                },
            },
        },
    },
    'selector_func': 'test.selector',
    'callback_func': 'test.callback',
}


def selector(method):
    backends = ['redis', 'dummy', 'invalid']
    return random.sample(backends, len(backends) - 1)


def callback(method, args, kwargs, results):
    print method, args, kwargs, results


backend = LazyServiceWrapper(
    BaseTSDB,
    backend_path,
    backend_opts,
)
