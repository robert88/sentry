from __future__ import absolute_import


class FutureSet(object):
    def __init__(self, futures):
        self.__pending = set(futures)
        self.__completed = set()
        self.__callbacks = []

        for future in futures:
            future.add_done_callback(self.__mark_completed)

    def __iter__(self):
        return iter(self.__pending | self.__completed)

    def __execute_callback(self, callback):
        try:
            callback(self)
        except Exception:
            # TODO: Log this or something?
            pass

    def __mark_completed(self, future):
        self.__pending.remove(future)
        self.__completed.add(future)
        if len(self.__pending) == 0:
            for callback in self.__callbacks:
                self.__execute_callback(callback)

    def add_done_callback(self, callback):
        if len(self.__pending) == 0:
            self.__execute_callback(callback)
        else:
            self.__callbacks.append(callback)
