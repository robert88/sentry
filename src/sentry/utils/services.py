from __future__ import absolute_import

import inspect
import itertools

from collections import OrderedDict
from django.utils.functional import empty, LazyObject

from sentry.exceptions import InvalidConfiguration
from sentry.utils import warnings

from .imports import import_string


class Service(object):
    __all__ = ()

    def validate(self):
        """
        Validates the settings for this backend (i.e. such as proper connection
        info).

        Raise ``InvalidConfiguration`` if there is a configuration error.
        """

    def setup(self):
        """
        Initialize this service.
        """


class LazyServiceWrapper(LazyObject):
    """
    Lazyily instantiates a standard Sentry service class.

    >>> LazyServiceWrapper(BaseClass, 'path.to.import.Backend', {})

    Provides an ``expose`` method for dumping public APIs to a context, such as
    module locals:

    >>> service = LazyServiceWrapper(...)
    >>> service.expose(locals())
    """

    def __init__(self, backend_base, backend_path, options, dangerous=()):
        super(LazyServiceWrapper, self).__init__()
        self.__dict__.update(
            {
                '_backend': backend_path,
                '_options': options,
                '_base': backend_base,
                '_dangerous': dangerous,
            }
        )

    def __getattr__(self, name):
        if self._wrapped is empty:
            self._setup()
        return getattr(self._wrapped, name)

    def _setup(self):
        backend = import_string(self._backend)
        assert issubclass(backend, Service)
        if backend in self._dangerous:
            warnings.warn(
                warnings.UnsupportedBackend(
                    u'The {!r} backend for {} is not recommended '
                    'for production use.'.format(self._backend, self._base)
                )
            )
        instance = backend(**self._options)
        self._wrapped = instance

    def expose(self, context):
        base = self._base
        for key in itertools.chain(base.__all__, ('validate', 'setup')):
            if inspect.ismethod(getattr(base, key)):
                context[key] = (lambda f: lambda *a, **k: getattr(self, f)(*a, **k))(key)
            else:
                context[key] = getattr(base, key)


import functools
import threading
from concurrent.futures import Future
from Queue import Queue
from sentry.utils.concurrent import FutureSet


class ThreadedExecutor(object):
    def __init__(self, worker_count=1, maxsize=0):
        self.__worker_count = worker_count
        self.__workers = set([])
        self.__started = False
        self.__queue = Queue(maxsize)

    def start(self):
        assert not self.__started

        def worker():
            queue = self.__queue
            while True:
                function, future = queue.get(True)
                try:
                    result = function()
                except Exception as error:
                    future.set_exception(error)
                else:
                    future.set_result(result)
                queue.task_done()

        for i in xrange(self.__worker_count):
            t = threading.Thread(None, worker)
            t.daemon = True
            t.start()
            self.__workers.add(t)

        self.__started = True

    def submit(self, callable, block=True, timeout=None):
        if not self.__started:
            self.start()

        future = Future()
        try:
            self.__queue.put((callable, future), block, timeout)
        except Queue.Full as error:
            if future.set_running_or_notify_cancel():
                future.set_exception(error)
        return future


class ServiceDelegator(Service):
    """
    - Only method access is delegated to the individual backends. Attribute
      values are returned from the base backend. Only methods that are defined
      on the base backend are eligble for delegation (since these methods are
      considered the public API.)
    - The backend makes no attempt to synchronize common backend option values
      between backends (e.g. TSDB rollup configuration) to ensure equivalency
      of request parameters based on configuration.
    - The return value of the selector function is a list of strings, which
      correspond to names in the backend mapping. The first item in the result
      list is considered the "primary backend". The remainder of the items in
      the result list are considered "secondary backends". The result value of
      the primary backend will be the result value of the delegated method (to
      callers, this appears as a synchronous method call.) The secondary
      backends are called asynchronously in the background. (To recieve the
      result values of these method calls, provide a callback_func, described
      below.) If the primary backend name returned by the selector function
      doesn't correspond to any registered backend, the function will raise a
      _____ (what?) exception. If any referenced secondary backends are not
      registered names, they will be discarded and logged to the ____ (which?)
      logger.
    - Each backend is associated with an executor pool with a single worker to
      ensure exclusive access to any sockets, etc. Each executor is started
      when the first task is submitted.
    - The request is added to the request queue of the primary backend using a
      blocking put. The request is added to the request queue(s) of the
      secondary backend(s) as a non-blocking put (if these queues are full, the
      request is rejected and the future will raise ``Queue.Full``.)
    - The callback_func is called with ______ (what?) after all requests have
      been completed (either succesfully or unsuccesfully) or been rejected.
    """

    def __init__(self, backend_base, backends, selector_func, callback_func=None):
        if callback_func is not None:
            callback_func = import_string(callback_func)

        self.__backend_base = import_string(backend_base)

        def load_executor(options):
            path = options.get('path')
            if path is None:
                executor_cls = ThreadedExecutor
            else:
                executor_cls = import_string(path)
            return executor_cls(**options.get('options', {}))

        # TODO: The added layer of indirection from the interior
        # LazyServiceWrapper is probably overkill here -- though it does allow
        # deferring the instantiation of the backend until ``validate`` is
        # called, which is possibly beneficial for ensuring that sockets, etc.
        # are established in the correct process in prefork environments...
        # though this might also (may likely?) be redundant with the top level
        # LazyServiceWrapper ...?
        self.__backends = {}
        for name, options in backends.items():
            self.__backends[name] = (
                LazyServiceWrapper(backend_base, options['path'], options.get('options', {})),
                load_executor(options.get('executor', {})),
            )

        self.__selector_func = import_string(selector_func)
        self.__callback_func = callback_func

    def validate(self):
        results = {}
        for backend, executor in self.__backends.values():
            try:
                backend.validate()
            except Exception as error:
                results[backend] = error

        if results:
            raise InvalidConfiguration(results.items())  # TODO: Improve formatting

    def setup(self):
        for backend, executor in self.__backends.values():
            backend.setup()

    def __getattr__(self, attribute_name):
        base_value = getattr(self.__backend_base, attribute_name)
        if not inspect.ismethod(base_value):
            return base_value

        def execute(*args, **kwargs):
            results = OrderedDict()

            for i, backend_name in enumerate(self.__selector_func(attribute_name)):
                is_primary = i == 0
                try:
                    backend, executor = self.__backends[backend_name]
                except KeyError:
                    if is_primary:
                        raise
                    else:
                        continue

                method = getattr(backend, attribute_name)
                results[backend_name] = executor.submit(
                    functools.partial(method, *args, **kwargs),
                    block=is_primary,
                )

            if self.__callback_func is not None:
                FutureSet(results.values()).add_done_callback(
                    lambda *a, **k: self.__callback_func(attribute_name, args, kwargs, results)
                )

            return results.values()[0].result()

        return execute
