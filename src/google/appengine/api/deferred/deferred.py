# Copyright 2009 Google Inc. All Rights Reserved.
"""A module that handles deferred execution of callables via the task queue.

Tasks consist of a callable and arguments to pass to it. The callable and its
arguments are serialized and put on the task queue, which deserializes and
executes them. The following callables can be used as tasks:

1) Functions defined in the top level of a module
2) Classes defined in the top level of a module
3) Instances of classes in (2) that implement __call__
4) Instance methods of objects of classes in (2)
5) Class methods of classes in (2)
6) Built-in functions
7) Built-in methods

The following callables can NOT be used as tasks:
1) Nested functions or closures
2) Nested classes or objects of them
3) Lambda functions
4) Static methods

The arguments to the callable, and the object (in the case of method or object
calls) must all be pickleable.

If you want your tasks to execute reliably, don't use mutable global variables;
they are not serialized with the task and may not be the same when your task
executes as they were when it was enqueued (in fact, they will almost certainly
be different).

If your app relies on manipulating the import path, make sure that the function
you are deferring is defined in a module that can be found without import path
manipulation. Alternately, you can include deferred.TaskHandler in your own
webapp application instead of using the easy-install method detailed below.

When you create a deferred task using deferred.defer, the task is serialized,
and an attempt is made to add it directly to the task queue. If the task is too
big (larger than about 10 kilobytes when serialized), a datastore entry will be
created for the task, and a new task will be enqueued, which will fetch the
original task from the datastore and execute it. This is much less efficient
than the direct execution model, so it's a good idea to minimize the size of
your tasks when possible.

In order for tasks to be processed, you need to set up the handler. Add the
following to your app.yaml handlers section:

handlers:
- url: /_ah/queue/deferred
  script: $PYTHON_LIB/google/appengine/ext/deferred/handler.py
  login: admin

By default, the deferred module uses the URL above, and the default queue.

Example usage::

    def do_something_later(key, amount):
      entity = MyModel.get(key)
      entity.total += amount
      entity.put()

    # Use default URL and queue name, no task name, execute ASAP.
    deferred.defer(do_something_later, my_key, 20)

    # Providing non-default task queue arguments
    deferred.defer(do_something_later, my_key, 20, _queue="foo", _countdown=60)
"""

# WARNING: This file is externally viewable by our users.  All comments from
# this file will be stripped.  The docstrings will NOT.  Do not put sensitive
# information in docstrings.  If you must communicate internal information in
# this source file, please place them in comments only.

import logging
import os
import pickle
import types
from google.appengine.api import taskqueue
from google.appengine.ext import db


_DEFAULT_LOG_LEVEL = logging.INFO
_TASKQUEUE_HEADERS = {"Content-Type": "application/octet-stream"}
_DEFAULT_URL = "/_ah/queue/deferred"
_DEFAULT_QUEUE = "default"


class Error(Exception):
  """Base class for exceptions in this module."""


class PermanentTaskFailure(Error):
  """Indicates that a task failed, and will never succeed."""


class SingularTaskFailure(Error):
  """Indicates that a task failed once."""


def set_log_level(log_level):
  """Sets the log level deferred will log to in normal circumstances.

  Args:
    log_level: one of logging log levels, e.g. logging.DEBUG, logging.INFO, etc.
  """
  global _DEFAULT_LOG_LEVEL
  _DEFAULT_LOG_LEVEL = log_level


def run(data):
  """Unpickles and executes a task.

  Args:
    data: A pickled tuple of (function, args, kwargs) to execute.

  Returns:
    The return value of the function invocation.
  """
  try:
    func, args, kwds = pickle.loads(data)
  except Exception as e:
    raise PermanentTaskFailure(e)
  else:
    return func(*args, **kwds)


class _DeferredTaskEntity(db.Model):
  """Datastore representation of a deferred task.

  This is used in cases when the deferred task is too big to be included as
  payload with the task queue entry.
  """
  data = db.BlobProperty(required=True)


def run_from_datastore(key):
  """Retrieves a task from the datastore and executes it.

  Args:
    key: The datastore key of a _DeferredTaskEntity storing the task.

  Returns:
    The return value of the function invocation.
  """
  entity = _DeferredTaskEntity.get(key)
  if not entity:
    # If the entity is missing, no number of retries will help.
    raise PermanentTaskFailure()
  try:
    ret = run(entity.data)
    entity.delete()
  except PermanentTaskFailure:
    entity.delete()
    raise


def invoke_member(obj, membername, *args, **kwargs):
  """Retrieves a member of an object, then calls it with the provided arguments.

  Args:
    obj: The object to operate on.
    membername: The name of the member to retrieve from ojb.
    args: Positional arguments to pass to the method.
    kwargs: Keyword arguments to pass to the method.

  Returns:
    The return value of the method invocation.
  """
  return getattr(obj, membername)(*args, **kwargs)


def _curry_callable(obj, *args, **kwargs):
  """Takes a callable and arguments and returns a task queue tuple.

  The returned tuple consists of (callable, args, kwargs), and can be pickled
  and unpickled safely.

  Args:
    obj: The callable to curry. See the module docstring for restrictions.
    args: Positional arguments to call the callable with.
    kwargs: Keyword arguments to call the callable with.

  Returns:
    A tuple consisting of  (callable, args, kwargs) that can be evaluated by
    run() with equivalent effect of executing the function directly.
  Raises:
    ValueError: If the passed in object is not of a valid callable type.
  """
  if isinstance(obj, types.MethodType):
    return (invoke_member, (obj.__self__, obj.__func__.__name__) + args, kwargs)
  elif isinstance(obj, types.BuiltinMethodType):
    if not obj.__self__:
      # Some built in functions are identified as methods (eg, operator.add)
      return (obj, args, kwargs)
    else:
      # In Python 3 the __self__ property of operator.add is a module object,
      # which cannot be pickled.
      if isinstance(obj.__self__, types.ModuleType):
        return (obj, args, kwargs)
      else:
        return (invoke_member, (obj.__self__, obj.__name__) + args, kwargs)
  elif isinstance(obj, object) and hasattr(obj, "__call__"):
    return (obj, args, kwargs)
  elif isinstance(obj, (types.FunctionType, types.BuiltinFunctionType, type,
                        types.UnboundMethodType)):
    return (obj, args, kwargs)
  else:
    raise ValueError("obj must be callable")


def serialize(obj, *args, **kwargs):
  """Serializes a callable into a format recognized by the deferred executor.

  Args:
    obj: The callable to serialize. See module docstring for restrictions.
    args: Positional arguments to call the callable with.
    kwargs: Keyword arguments to call the callable with.

  Returns:
    A serialized representation of the callable.
  """
  curried = _curry_callable(obj, *args, **kwargs)
  return pickle.dumps(curried, protocol=pickle.HIGHEST_PROTOCOL)


def defer(obj, *args, **kwargs):
  """Defers a callable for execution later.

  The default deferred URL of /_ah/queue/deferred will be used unless an
  alternate URL is explicitly specified. If you want to use the default URL for
  a queue, specify _url=None. If you specify a different URL, you will need to
  install the handler on that URL (see the module docstring for details).

  Args:
    obj: The callable to execute. See module docstring for restrictions.
      _countdown, _eta, _headers, _name, _target, _transactional, _url,
        _retry_options, _queue: Passed through to the task queue - see the task
          queue documentation for details.
    args: Positional arguments to call the callable with.
    kwargs: Any other keyword arguments are passed through to the callable.

  Returns:
    A taskqueue.Task object which represents an enqueued callable.
  """
  taskargs = dict(
      (x, kwargs.pop(("_%s" % x), None))
      for x in ("countdown", "eta", "name", "target", "retry_options"))
  taskargs["url"] = kwargs.pop("_url", _DEFAULT_URL)
  transactional = kwargs.pop("_transactional", False)
  taskargs["headers"] = dict(_TASKQUEUE_HEADERS)
  taskargs["headers"].update(kwargs.pop("_headers", {}))
  queue = kwargs.pop("_queue", _DEFAULT_QUEUE)
  pickled = serialize(obj, *args, **kwargs)
  try:
    task = taskqueue.Task(payload=pickled, **taskargs)
    return task.add(queue, transactional=transactional)
  except taskqueue.TaskTooLargeError:
    # Task is too big - store it to the datastore
    key = _DeferredTaskEntity(data=pickled).put()
    pickled = serialize(run_from_datastore, str(key))
    task = taskqueue.Task(payload=pickled, **taskargs)
    return task.add(queue)


def _execute_deferred_task(environ, start_response):
  """Default behavior for POST requests to deferred handler."""

  print(environ)

  # Protect against XSRF attacks
  if "HTTP_X-APPENGINE-TASKNAME" not in environ:
    logging.error("Detected an attempted XSRF attack. The header "
                  '"X-AppEngine-Taskname" was not set.')
    start_response("403 Forbidden", [])
    yield "Detected an attempted XSRF attack. The header X-AppEngine-Taskname was not set."

  # Since administrators of an app can set the X-AppEngine-TaskName header, we
  # also ensure that this task comes from the TaskQueue IP address.
  in_prod = (
      not environ.get("SERVER_SOFTWARE").startswith("Devel"))
  if in_prod and environ.get("REMOTE_ADDR") != "0.1.0.2":
      logging.error("Detected an attempted XSRF attack. This request did "
                  "not originate from Task Queue.")
      start_response("403 Forbidden", [])
      yield "Detected an attempted XSRF attack. This request did not originate from Task Queue."

  # Log some information about the task we're executing
  headers = [
      "%s:%s" % (k[5:], v)
      for k, v in environ.items()
      if k.upper().startswith("HTTP_X-APPENGINE-")
  ]
  logging.log(_DEFAULT_LOG_LEVEL, ", ".join(headers))

  # the environment variable CONTENT_LENGTH may be empty or missing
  try:
    request_body_size = int(environ.get('CONTENT_LENGTH', 0))
  except (ValueError):
    request_body_size = 0

  request_body = environ['wsgi.input'].read(request_body_size)
  run(request_body)
  start_response("200 OK", [])
  yield "Success"

def execute_deferred_task(environ, start_response):
  try:
    _execute_deferred_task(environ, start_response)
  except SingularTaskFailure:
    start_response("408 SingularTaskFailure", [])
    yield "SingularTaskFailure occurred"
    # Catch a SingularTaskFailure. Intended for users to be able to force a
    # task retry without causing an error.
    logging.debug("Failure executing task, task retry forced")
  except PermanentTaskFailure:
    # Catch this so we return a 200 and don't retry the task.
    start_response("200 PermanentTaskFailure", [])
    yield "PermanentTaskFailure"
    logging.exception("Permanent failure attempting to execute task")

