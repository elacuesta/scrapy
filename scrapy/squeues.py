"""
Scheduler queues
"""

import marshal
import os
import pickle

from queuelib import queue

from scrapy.utils.deprecate import create_deprecated_class
from scrapy.utils.reqser import request_from_dict, request_to_dict


def _with_mkdir(queue_class):

    class DirectoriesCreated(queue_class):
        def __init__(self, path, *args, **kwargs):
            dirname = os.path.dirname(path)
            if not os.path.exists(dirname):
                os.makedirs(dirname, exist_ok=True)
            super().__init__(path, *args, **kwargs)

    return DirectoriesCreated


def _serialization_queue(queue_class, serialize, deserialize):

    class SerializationQueue(queue_class):
        def push(self, obj):
            super().push(serialize(obj))

        def pop(self):
            s = super().pop()
            return deserialize(s) if s else None

        def peek(self):
            try:
                s = super().peek()
            except AttributeError as ex:
                raise NotImplementedError("The underlying queue class does not implement 'peek'") from ex
            return deserialize(s) if s else None

    return SerializationQueue


class _BaseMemoryQueue:
    """
    Base general-purpose queue that stores elements in memory
    """
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        return cls()

    def peek(self):
        try:
            return super().peek()
        except AttributeError as ex:
            raise NotImplementedError("The underlying queue class does not implement 'peek'") from ex


class _BaseDiskRequestQueue:
    """
    Base queue for requests that stores elements to disk and converts requests to/from dictionaries
    """
    def __init__(self, crawler, key):
        self.spider = crawler.spider
        super().__init__(key)

    @classmethod
    def from_crawler(cls, crawler, key, *args, **kwargs):
        return cls(crawler, key)

    def push(self, request):
        request = request_to_dict(request, self.spider)
        return super().push(request)

    def pop(self):
        request = super().pop()
        return request_from_dict(request, self.spider) if request else None

    def peek(self):
        request = super().peek()  # NotImplementedError could be raised from the underlying queue
        return request_from_dict(request, self.spider) if request else None


def _pickle_serialize(obj):
    try:
        return pickle.dumps(obj, protocol=4)
    # Both pickle.PicklingError and AttributeError can be raised by pickle.dump(s)
    # TypeError is raised from parsel.Selector
    except (pickle.PicklingError, AttributeError, TypeError) as e:
        raise ValueError(str(e)) from e


_PickleFifoSerializationDiskQueue = _serialization_queue(
    queue_class=_with_mkdir(queue.FifoDiskQueue),
    serialize=_pickle_serialize,
    deserialize=pickle.loads,
)
_PickleLifoSerializationDiskQueue = _serialization_queue(
    queue_class=_with_mkdir(queue.LifoDiskQueue),
    serialize=_pickle_serialize,
    deserialize=pickle.loads,
)
_MarshalFifoSerializationDiskQueue = _serialization_queue(
    queue_class=_with_mkdir(queue.FifoDiskQueue),
    serialize=marshal.dumps,
    deserialize=marshal.loads,
)
_MarshalLifoSerializationDiskQueue = _serialization_queue(
    queue_class=_with_mkdir(queue.LifoDiskQueue),
    serialize=marshal.dumps,
    deserialize=marshal.loads,
)


# usable queue classes
FifoMemoryQueue = type("FifoMemoryQueue", (_BaseMemoryQueue, queue.FifoMemoryQueue), {})
LifoMemoryQueue = type("LifoMemoryQueue", (_BaseMemoryQueue, queue.LifoMemoryQueue), {})
PickleFifoDiskQueue = type("PickleFifoDiskQueue", (_BaseDiskRequestQueue, _PickleFifoSerializationDiskQueue), {})
PickleLifoDiskQueue = type("PickleLifoDiskQueue", (_BaseDiskRequestQueue, _PickleLifoSerializationDiskQueue), {})
MarshalFifoDiskQueue = type("MarshalFifoDiskQueue", (_BaseDiskRequestQueue, _MarshalFifoSerializationDiskQueue), {})
MarshalLifoDiskQueue = type("MarshalLifoDiskQueue", (_BaseDiskRequestQueue, _MarshalLifoSerializationDiskQueue), {})


# deprecated classes
subclass_warn_message = "{cls} inherits from deprecated class {old}"
instance_warn_message = "{cls} is deprecated"
PickleFifoDiskQueueNonRequest = create_deprecated_class(
    name="PickleFifoDiskQueueNonRequest",
    new_class=_PickleFifoSerializationDiskQueue,
    subclass_warn_message=subclass_warn_message,
    instance_warn_message=instance_warn_message,
)
PickleLifoDiskQueueNonRequest = create_deprecated_class(
    name="PickleLifoDiskQueueNonRequest",
    new_class=_PickleLifoSerializationDiskQueue,
    subclass_warn_message=subclass_warn_message,
    instance_warn_message=instance_warn_message,
)
MarshalFifoDiskQueueNonRequest = create_deprecated_class(
    name="MarshalFifoDiskQueueNonRequest",
    new_class=_MarshalFifoSerializationDiskQueue,
    subclass_warn_message=subclass_warn_message,
    instance_warn_message=instance_warn_message,
)
MarshalLifoDiskQueueNonRequest = create_deprecated_class(
    name="MarshalLifoDiskQueueNonRequest",
    new_class=_MarshalLifoSerializationDiskQueue,
    subclass_warn_message=subclass_warn_message,
    instance_warn_message=instance_warn_message,
)
