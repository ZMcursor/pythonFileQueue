"""A multi-producer, multi-consumer queue based on file system"""

import os
import tempfile
import threading
from collections import deque
from time import time as _time

try:
    import cPickle as _pickle
except ImportError:
    import pickle as _pickle
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty

__all__ = ["Empty", "FileQueue"]


class FileQueue(object):
    """ Create a queue object with the given 'buffer_dir' and 'buffer_size'.

    When the size of queue is reach the 'buffer_size',then the queue
    will be pickled to a file under the 'buffer_dir'.
    'buffer_dir' will be created if neccesary and if 'buffer_dir' is not
    given,then the Temporary directory will be use.
    """

    def __init__(self, buffer_dir=None, buffer_size=100000, save_data=False):
        if buffer_dir:
            self.__buffer_dir = buffer_dir
        else:
            self.__buffer_dir = os.path.join(
                tempfile.gettempdir(), 'pyFileQueue')
        self.__buffer_size = buffer_size
        # whether to save data when closing
        self.__save_data = save_data
        # create buffer_dir if neccesary
        if not os.path.exists(self.__buffer_dir):
            os.makedirs(self.__buffer_dir)
        info_path = os.path.join(self.__buffer_dir, 'info')
        if os.path.exists(info_path):
            # load info
            with open(info_path, 'rb') as f:
                self.__size, self.__files = _pickle.load(f)
        else:
            self.__size = 0
            self.__files = deque()

        # queue to put item
        self.__queue_in = deque()
        # queue to get item
        self.__queue_out = deque()
        self.__mutex = threading.Lock()
        self.__not_empty = threading.Condition(self.__mutex)

    def put(self, item):
        """Put an item into the queue.

        The queue will pickled to a file if its size reach the buffer_size.
        """
        self.__mutex.acquire()
        try:
            self.__queue_in.append(item)
            if len(self.__queue_in) >= self.__buffer_size:
                if not self.__files and not self.__queue_out:
                    self.__queue_in, self.__queue_out = self.__queue_out, self.__queue_in
                else:
                    self.__save_to_file(self.__queue_in)
                    self.__queue_in = deque()
            self.__size += 1
            self.__not_empty.notify()
        finally:
            self.__mutex.release()

    def get(self, block=True, timeout=0):
        """Remove and return an item from the queue.

        If optional args 'block' is true,block if necessary until an item
        is available. If 'timeout' is greater than 0, it blocks at most
        'timeout' seconds and raises the Empty exception if no item was
        available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).
        """
        self.__not_empty.acquire()
        try:
            if not self.__queue_out:
                if self.__files:
                    self.__queue_out = self.__get_from_file(
                        self.__files.popleft())
                elif self.__queue_in:
                    self.__queue_in, self.__queue_out = self.__queue_out, self.__queue_in
                else:
                    if block:
                        if timeout > 0:
                            end = _time() + timeout
                            while not self.__queue_in:
                                remaining = end - _time()
                                if remaining <= 0.0:
                                    raise Empty
                                self.__not_empty.wait(remaining)
                        else:
                            while not self.__queue_in:
                                self.__not_empty.wait()
                        self.__queue_in, self.__queue_out = self.__queue_out, self.__queue_in
                    else:
                        raise Empty
            self.__size -= 1
            return self.__queue_out.popleft()
        finally:
            self.__not_empty.release()

    def get_nowait(self):
        """Remove and return an item from the queue without blocking.

        Only get an item if one is immediately available. Otherwise
        raise the Empty exception.
        """
        return self.get(block=False)

    def size(self):
        """Return the approximate size of the queue (not reliable!)."""
        return self.__size

    @property
    def buffer_dir(self):
        """Return the buffer directory."""
        return self.__buffer_dir

    @property
    def buffer_size(self):
        """Return the buffer size."""
        return self.__buffer_size

    @property
    def is_save_data(self):
        """Return if save data."""
        return self.__save_data

    def __save_to_file(self, queue, tail=True):
        """Pickled a queue to a file"""
        file_name = str(int(_time() * 1000))
        if tail:
            self.__files.append(file_name)
        else:
            self.__files.appendleft(file_name)
        with open(os.path.join(self.__buffer_dir, file_name), 'wb') as f:
            _pickle.dump(queue, f, -1)

    def __get_from_file(self, filename):
        """Load a queue from file"""
        file_path = os.path.join(self.__buffer_dir, filename)
        with open(file_path, 'rb') as f:
            obj = _pickle.load(f)
        os.remove(file_path)
        return obj

    def close(self):
        """Save data and remove useless file if naccesary.

        if 'save_data' is True,queue will be saved.Otherwise the 
        buffer_dir will be remove.
        """
        def rmdir(dir_path):
            for item in os.listdir(dir_path):
                path = os.path.join(dir_path, item)
                if os.path.isdir(path):
                    rmdir(path)
                else:
                    os.remove(path)
            os.rmdir(dir_path)

        if self.__save_data and self.__files is not None:
            if self.__queue_in:
                self.__save_to_file(self.__queue_in)
            if self.__queue_out:
                self.__save_to_file(self.__queue_out, False)
            if self.__files:
                with open(os.path.join(self.__buffer_dir, 'info'), 'wb') as f:
                    _pickle.dump((self.__size, self.__files), f, -1)
            else:
                rmdir(self.__buffer_dir)
        else:
            rmdir(self.__buffer_dir)
        self.__queue_in = None
        self.__queue_out = None
        self.__files = None

    def __len__(self):
        return self.__size

    def __repr__(self):
        return 'FileQueue(localtion:%s, size:%d)' % (self.__buffer_dir, self.__size)

    def __del__(self):
        self.close()
