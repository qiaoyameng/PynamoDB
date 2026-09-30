"""
DynamoDB Streams record decoding.

This module provides :class:`StreamRecord`, the result of decoding a DynamoDB Streams
record against a :class:`~pynamodb.models.Model` class via
:meth:`Model.from_stream_record <pynamodb.models.Model.from_stream_record>`.
"""
from typing import Any
from typing import Dict
from typing import Generic
from typing import Optional
from typing import Type
from typing import TypeVar

from pynamodb.attributes import Attribute
from pynamodb.attributes import AttributeContainer
from pynamodb.attributes import MapAttribute
from pynamodb.constants import (
    APPROXIMATE_CREATION_DATE_TIME,
    KEYS,
    NEW_IMAGE,
    NULL,
    OLD_IMAGE,
    SEQUENCE_NUMBER,
    SIZE_BYTES,
    STREAM_AWS_REGION,
    STREAM_EVENT_ID,
    STREAM_EVENT_NAME,
    STREAM_EVENT_SOURCE,
    STREAM_EVENT_SOURCE_ARN,
    STREAM_EVENT_VERSION,
    STREAM_RECORD,
    STREAM_USER_IDENTITY,
    STREAM_VIEW_TYPE,
)
from pynamodb.exceptions import StreamRecordDeserializationError

_ACT = TypeVar('_ACT', bound=AttributeContainer)

OLD_IMAGE_LABEL = 'old_image'
NEW_IMAGE_LABEL = 'new_image'
KEYS_LABEL = 'keys'


class StreamRecord(Generic[_ACT]):
    """
    A DynamoDB Streams record decoded against a model class.

    Depending on the event type and the stream view type, some images may be absent;
    a missing image is represented as :code:`None` and never as a model instance
    populated with default values:

    - ``INSERT`` events carry a :code:`new_image`.
    - ``MODIFY`` events carry both an :code:`old_image` and a :code:`new_image`.
    - ``REMOVE`` events carry an :code:`old_image`.
    - :code:`keys` holds the primary key of the affected item, when present.

    Instances decoded from an image only contain the attributes actually present in
    the image; no defaults are applied. When decoded with
    :code:`keep_unknown_attributes=True`, attributes present in the image but not
    declared on the model are preserved in their raw form on the instance's
    :code:`_unknown_attributes` dictionary.
    """

    def __init__(
        self,
        *,
        event_name: Optional[str] = None,
        old_image: Optional[_ACT] = None,
        new_image: Optional[_ACT] = None,
        keys: Optional[_ACT] = None,
        sequence_number: Optional[str] = None,
        size_bytes: Optional[int] = None,
        stream_view_type: Optional[str] = None,
        approximate_creation_date_time: Any = None,
        event_id: Optional[str] = None,
        event_version: Optional[str] = None,
        event_source: Optional[str] = None,
        event_source_arn: Optional[str] = None,
        aws_region: Optional[str] = None,
        user_identity: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.event_name = event_name
        self.old_image = old_image
        self.new_image = new_image
        self.keys = keys
        self.sequence_number = sequence_number
        self.size_bytes = size_bytes
        self.stream_view_type = stream_view_type
        self.approximate_creation_date_time = approximate_creation_date_time
        self.event_id = event_id
        self.event_version = event_version
        self.event_source = event_source
        self.event_source_arn = event_source_arn
        self.aws_region = aws_region
        self.user_identity = user_identity

    def __repr__(self) -> str:
        return "StreamRecord(event_name={}, sequence_number={})".format(
            self.event_name, self.sequence_number)


def decode_stream_record(
    model_cls: Type[_ACT],
    record: Dict[str, Any],
    keep_unknown_attributes: bool = False,
) -> StreamRecord[_ACT]:
    """
    Decodes a DynamoDB Streams record into a :class:`StreamRecord`.

    :param model_cls: the model class to decode images into
    :param record: a stream record as returned by ``dynamodbstreams.get_records``
      or delivered to a Lambda function (the full record, or just its ``dynamodb`` body)
    :param keep_unknown_attributes: if :code:`True`, attributes present in an image but
      not declared on the model are preserved in their raw form on the decoded instance's
      :code:`_unknown_attributes` dictionary; otherwise they are silently dropped
    """
    if record is None:
        raise ValueError("Received no stream record to decode")

    stream_data = record.get(STREAM_RECORD, record) or {}
    return StreamRecord(
        event_name=record.get(STREAM_EVENT_NAME),
        old_image=_deserialize_image(model_cls, stream_data.get(OLD_IMAGE), OLD_IMAGE_LABEL,
                                     keep_unknown_attributes),
        new_image=_deserialize_image(model_cls, stream_data.get(NEW_IMAGE), NEW_IMAGE_LABEL,
                                     keep_unknown_attributes),
        keys=_deserialize_image(model_cls, stream_data.get(KEYS), KEYS_LABEL,
                                keep_unknown_attributes),
        sequence_number=stream_data.get(SEQUENCE_NUMBER),
        size_bytes=stream_data.get(SIZE_BYTES),
        stream_view_type=stream_data.get(STREAM_VIEW_TYPE),
        approximate_creation_date_time=stream_data.get(APPROXIMATE_CREATION_DATE_TIME),
        event_id=record.get(STREAM_EVENT_ID),
        event_version=record.get(STREAM_EVENT_VERSION),
        event_source=record.get(STREAM_EVENT_SOURCE),
        event_source_arn=record.get(STREAM_EVENT_SOURCE_ARN),
        aws_region=record.get(STREAM_AWS_REGION),
        user_identity=record.get(STREAM_USER_IDENTITY),
    )


def _deserialize_image(
    model_cls: Type[_ACT],
    attribute_values: Optional[Dict[str, Dict[str, Any]]],
    image: str,
    keep_unknown_attributes: bool,
) -> Optional[_ACT]:
    if not attribute_values:
        # A missing image is legitimate (e.g. no OldImage on INSERT); do not
        # fabricate a model instance populated with default values.
        return None

    stored_cls = model_cls._get_discriminator_class(attribute_values)  # type: ignore
    if stored_cls and not issubclass(stored_cls, model_cls):
        raise ValueError("Cannot instantiate a {} from the returned class: {}".format(
            model_cls.__name__, stored_cls.__name__))
    target_cls = stored_cls or model_cls

    instance = target_cls(_user_instantiated=False)
    # Unlike `Model.from_raw_data`, a stream image may be partial (e.g. KEYS_ONLY):
    # clear any defaults so the instance only reflects what is actually in the image.
    instance.attribute_values = {}
    instance._set_discriminator()

    known_attr_names = set()
    for name, attr in instance.get_attributes().items():
        known_attr_names.add(attr.attr_name)
        raw_value = attribute_values.get(attr.attr_name)
        if raw_value and NULL not in raw_value:
            setattr(instance, name, _deserialize_attribute(attr, raw_value, name, image))

    if keep_unknown_attributes:
        instance._unknown_attributes = {  # type: ignore
            attr_name: value for attr_name, value in attribute_values.items()
            if attr_name not in known_attr_names
        }
    return instance


def _deserialize_attribute(
    attr: Attribute,
    raw_value: Dict[str, Any],
    path: str,
    image: str,
) -> Any:
    try:
        value = attr.get_value(raw_value)
    except Exception as e:
        raise StreamRecordDeserializationError(image=image, attr_path=path, cause=e) from e
    try:
        return attr.deserialize(value)
    except Exception as e:
        if isinstance(attr, MapAttribute) and not attr.is_raw() and isinstance(value, dict):
            # Descend into typed maps to pinpoint the nested attribute that failed.
            _deserialize_map_children(attr, value, path, image)
        raise StreamRecordDeserializationError(image=image, attr_path=path, cause=e) from e


def _deserialize_map_children(
    map_attr: MapAttribute,
    values: Dict[str, Dict[str, Any]],
    path: str,
    image: str,
) -> None:
    for name, sub_attr in map_attr.get_attributes().items():
        raw_value = values.get(sub_attr.attr_name)
        if raw_value and NULL not in raw_value:
            _deserialize_attribute(sub_attr, raw_value, path + '.' + name, image)
