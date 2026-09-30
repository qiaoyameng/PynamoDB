DynamoDB Streams
================

PynamoDB can decode a DynamoDB Streams record directly into model instances.

.. code-block:: python

    from my_models import Order

    def handler(event, context):
        for raw_record in event['Records']:
            record = Order.from_stream_record(raw_record)
            if record.event_name == 'REMOVE':
                order = record.old_image
            else:
                order = record.new_image

The decoded :class:`~pynamodb.streams.StreamRecord` always provides the primary key
as ``key``. The ``old_image`` and ``new_image`` values are ``None`` when that image is
not included in the record. For example, an ``INSERT`` record normally has no old image,
and a ``REMOVE`` record normally has no new image. Partial images intentionally do not
receive default attribute values.

The DynamoDB ``SequenceNumber``, ``SizeBytes``, ``StreamViewType``, and
``ApproximateCreationDateTime`` values are available as properties. The original
``dynamodb`` stream mapping and complete record are also retained.

If a field has been removed from the model but may still appear in an older stream
record, pass ``preserve_unknown_attributes=True``:

.. code-block:: python

    record = Order.from_stream_record(
        raw_record,
        preserve_unknown_attributes=True,
    )

Unknown attributes are decoded using DynamoDB's native type system and retained on the
model instance. By default they are omitted, matching the model definition.

Deserialization errors raise
:class:`~pynamodb.exceptions.StreamAttributeDeserializationError`, which includes the
failing image, the document path of the field (including nested map and list segments),
and the original deserialization error as ``__cause__``.
