"""
Model helpers for DynamoDB Streams records.
"""
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Generic
from typing import Optional
from typing import Type
from typing import TypeVar

from pynamodb.attributes import AttributeContainer
from pynamodb.models import Model


_M = TypeVar('_M', bound='Model')
_MT = TypeVar('_MT', bound=Model)

INSERT = 'INSERT'
MODIFY = 'MODIFY'
REMOVE = 'REMOVE'

KEYS_IMAGE = 'Keys'
OLD_IMAGE = 'OldImage'
NEW_IMAGE = 'NewImage'


@dataclass(frozen=True)
class StreamRecord(Generic[_M]):
    """
    A DynamoDB Streams record decoded as a PynamoDB model.

    Missing images remain ``None``. For example, ``INSERT`` records normally have
    no old image and ``REMOVE`` records normally have no new image.
    """
    event_name: str
    key: _M
    old_image: Optional[_M]
    new_image: Optional[_M]
    sequence_number: Optional[str]
    size_bytes: Optional[int]
    stream_view_type: Optional[str]
    approximate_creation_date_time: Optional[Any]
    event_id: Optional[str]
    event_version: Optional[str]
    event_source: Optional[str]
    aws_region: Optional[str]
    user_identity: Optional[Dict[str, Any]]
    dynamodb: Dict[str, Any]
    record: Dict[str, Any]

    @property
    def keys(self) -> _M:
        return self.key

    @property
    def stream_record(self) -> Dict[str, Any]:
        return self.dynamodb


ModelStreamRecord = StreamRecord


def decode_stream_record(
    model_cls: Type[_MT],
    record: Dict[str, Any],
    *,
    preserve_unknown_attributes: bool = False,
) -> StreamRecord[_MT]:
    if not isinstance(model_cls, type) or not issubclass(model_cls, Model):
        raise TypeError("model_cls must be a subclass of Model")
    if not isinstance(record, dict):
        raise ValueError("record must be a DynamoDB Streams record mapping")

    dynamodb = record.get('dynamodb')
    if not isinstance(dynamodb, dict):
        raise ValueError("record is missing its 'dynamodb' mapping")

    event_name = record.get('eventName')
    if event_name not in (INSERT, MODIFY, REMOVE):
        raise ValueError("record has an invalid or missing 'eventName'")

    keys = dynamodb.get('Keys')
    if not isinstance(keys, dict):
        raise ValueError("record is missing its 'dynamodb.Keys' mapping")

    old_image = _decode_image(
        model_cls,
        dynamodb.get('OldImage'),
        OLD_IMAGE,
        preserve_unknown_attributes,
    )
    new_image = _decode_image(
        model_cls,
        dynamodb.get('NewImage'),
        NEW_IMAGE,
        preserve_unknown_attributes,
    )
    key = _decode_image(
        model_cls,
        keys,
        KEYS_IMAGE,
        preserve_unknown_attributes,
        check_required=True,
    )

    return StreamRecord(
        event_name=event_name,
        key=key,
        old_image=old_image,
        new_image=new_image,
        sequence_number=dynamodb.get('SequenceNumber'),
        size_bytes=dynamodb.get('SizeBytes'),
        stream_view_type=dynamodb.get('StreamViewType'),
        approximate_creation_date_time=dynamodb.get('ApproximateCreationDateTime'),
        event_id=record.get('eventID'),
        event_version=record.get('eventVersion'),
        event_source=record.get('eventSource'),
        aws_region=record.get('awsRegion'),
        user_identity=record.get('userIdentity'),
        dynamodb=dynamodb,
        record=record,
    )


def _decode_image(
    model_cls: Type[_MT],
    image: Optional[Dict[str, Dict[str, Any]]],
    image_name: str,
    preserve_unknown_attributes: bool,
    *,
    check_required: bool = False,
) -> Optional[AttributeContainer]:
    if image is None:
        return None
    if not isinstance(image, dict):
        raise ValueError("record contains an invalid {} image".format(image_name))
    if check_required and not image:
        raise ValueError("record contains an empty {} image".format(image_name))
    return model_cls._instantiate_from_stream(
        image,
        image_name=image_name,
        preserve_unknown_attributes=preserve_unknown_attributes,
    )
