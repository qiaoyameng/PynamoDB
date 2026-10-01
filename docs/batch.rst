Batch Operations
================

Batch operations are supported using context managers, and iterators. The DynamoDB API has limits for each batch operation
that it supports, but PynamoDB removes the need implement your own grouping or pagination. Instead, it handles
pagination for you automatically.


.. note::

    DynamoDB limits batch write operations to 25 `PutRequests` and `DeleteRequests` combined. `PynamoDB` automatically
    groups your writes 25 at a time for you.

Suppose that you have defined a `Thread` Model for the examples below.

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import (
        UnicodeAttribute, NumberAttribute
    )


    class Thread(Model):
        class Meta:
            table_name = 'Thread'

        forum_name = UnicodeAttribute(hash_key=True)
        subject = UnicodeAttribute(range_key=True)
        views = NumberAttribute(default=0)


Batch Writes
^^^^^^^^^^^^

Here is an example using a context manager for a bulk write operation:

.. code-block:: python

    with Thread.batch_write() as batch:
        items = [Thread('forum-{0}'.format(x), 'subject-{0}'.format(x)) for x in range(1000)]
        for item in items:
            batch.save(item)

Batch Gets
^^^^^^^^^^

Here is an example using an iterator for retrieving items in bulk:

.. code-block:: python

    item_keys = [('forum-{0}'.format(x), 'subject-{0}'.format(x)) for x in range(1000)]
    for item in Thread.batch_get(item_keys):
        print(item)

Ordered Batch Gets
^^^^^^^^^^^^^^^^^^

:meth:`Model.batch_get` yields items in whatever order DynamoDB returns them, and silently
de-duplicates your input. If you need the results to line up with the requested keys, use
:meth:`Model.batch_get_ordered` instead. It accepts duplicate keys (each unique key is still
only requested once) and returns a list with exactly one entry per requested key, in request
order. Each entry is either a model instance (a distinct instance per position, even for
duplicate keys) or a marker:

* ``pynamodb.models.BATCH_GET_NOT_FOUND`` -- the key was processed but no such item exists.
* ``pynamodb.models.BATCH_GET_UNKNOWN`` -- the key was still unprocessed after
  ``max_retries`` attempts (the default is 3), so its existence could not be determined.

.. code-block:: python

    from pynamodb.models import BATCH_GET_NOT_FOUND, BATCH_GET_UNKNOWN

    item_keys = [('forum-1', 'subject-1'), ('forum-2', 'subject-2'), ('forum-1', 'subject-1')]
    results = Thread.batch_get_ordered(item_keys, max_retries=3)
    for key, result in zip(item_keys, results):
        if result is BATCH_GET_NOT_FOUND:
            print('{} does not exist'.format(key))
        elif result is BATCH_GET_UNKNOWN:
            print('{} could not be fetched'.format(key))
        else:
            print(result)

Query Filters
^^^^^^^^^^^^^

You can query items from your table using a simple syntax:

.. code-block:: python

    for item in Thread.query('ForumName', Thread.subject.startswith('mygreatprefix')):
        print("Query returned item {0}".format(item))

Additionally, you can filter the results before they are returned using condition expressions:

.. code-block:: python

    for item in Thread.query('ForumName', Thread.subject == 'Subject', Thread.views > 0):
        print("Query returned item {0}".format(item))



Query filters use the condition expression syntax (see :ref:`conditions`).

.. note::

    DynamoDB only allows the following conditions on range keys: `==`, `<`, `<=`, `>`, `>=`, `between`, and `startswith`.
    DynamoDB does not allow multiple conditions using range keys.


Scan Filters
^^^^^^^^^^^^

Scan filters have the same syntax as Query filters, but support all condition expressions:

.. code-block:: python

    >>> for item in Thread.scan(Thread.forum_name.startswith('Prefix') & (Thread.views > 10)):
            print(item)

Limiting results
^^^^^^^^^^^^^^^^

Both `Scan` and `Query` results can be limited to a maximum number of items using the `limit` argument.

.. code-block:: python

    for item in Thread.query('ForumName', Thread.subject.startswith('mygreatprefix'), limit=5):
        print("Query returned item {0}".format(item))
