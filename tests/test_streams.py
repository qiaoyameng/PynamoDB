"""
Tests for DynamoDB Streams record decoding (Model.from_stream_record).
"""
from datetime import datetime

import pytest

from pynamodb.attributes import (
    BinaryAttribute,
    BinarySetAttribute,
    MapAttribute,
    NumberAttribute,
    NumberSetAttribute,
    UnicodeAttribute,
    UnicodeSetAttribute,
)
from pynamodb.exceptions import StreamRecordDeserializationError
from pynamodb.models import Model


class Address(MapAttribute):
    street = UnicodeAttribute()
    zip_code = NumberAttribute(attr_name='zip')


class StreamModel(Model):
    class Meta:
        table_name = 'stream-model'
        region = 'us-east-1'

    id = UnicodeAttribute(hash_key=True)
    version = NumberAttribute(range_key=True)
    name = UnicodeAttribute(default='default-name')
    count = NumberAttribute(null=True)
    payload = BinaryAttribute(legacy_encoding=False, null=True)
    attachments = BinarySetAttribute(legacy_encoding=False)
    tags = UnicodeSetAttribute()
    scores = NumberSetAttribute()
    address = Address(null=True)


def _image(**overrides):
    image = {
        'id': {'S': 'item-1'},
        'version': {'N': '2'},
        'name': {'S': 'alice'},
        'count': {'N': '42'},
        'payload': {'B': b'\x00\x01\x02'},
        'attachments': {'BS': [b'\x01', b'\x02']},
        'tags': {'SS': ['red', 'blue']},
        'scores': {'NS': ['1', '2.5']},
        'address': {'M': {'street': {'S': 'main st'}, 'zip': {'N': '12345'}}},
    }
    image.update(overrides)
    return image


def _record(event_name, keys=None, new_image=None, old_image=None, **stream_overrides):
    stream_data = {
        'SequenceNumber': '987654321',
        'SizeBytes': 128,
        'StreamViewType': 'NEW_AND_OLD_IMAGES',
        'ApproximateCreationDateTime': datetime(2026, 9, 30, 12, 0, 0),
    }
    stream_data.update(stream_overrides)
    if keys is not None:
        stream_data['Keys'] = keys
    if new_image is not None:
        stream_data['NewImage'] = new_image
    if old_image is not None:
        stream_data['OldImage'] = old_image
    return {
        'eventID': 'event-1',
        'eventName': event_name,
        'eventVersion': '1.1',
        'eventSource': 'aws:dynamodb',
        'eventSourceARN': 'arn:aws:dynamodb:us-east-1:123456789012:table/stream-model/stream/2026',
        'awsRegion': 'us-east-1',
        'dynamodb': stream_data,
    }


KEYS = {'id': {'S': 'item-1'}, 'version': {'N': '2'}}


def test_insert_record():
    record = StreamModel.from_stream_record(
        _record('INSERT', keys=KEYS, new_image=_image()))

    assert record.event_name == 'INSERT'
    assert record.sequence_number == '987654321'
    assert record.size_bytes == 128
    assert record.stream_view_type == 'NEW_AND_OLD_IMAGES'
    assert record.approximate_creation_date_time == datetime(2026, 9, 30, 12, 0, 0)
    assert record.event_id == 'event-1'
    assert record.event_version == '1.1'
    assert record.event_source == 'aws:dynamodb'
    assert record.event_source_arn.endswith('/2026')
    assert record.aws_region == 'us-east-1'

    assert record.old_image is None

    new_image = record.new_image
    assert new_image is not None
    assert new_image.id == 'item-1'
    assert new_image.version == 2
    assert new_image.name == 'alice'
    assert new_image.count == 42
    assert new_image.payload == b'\x00\x01\x02'
    assert new_image.attachments == {b'\x01', b'\x02'}
    assert new_image.tags == {'red', 'blue'}
    assert new_image.scores == {1, 2.5}
    assert isinstance(new_image.address, Address)
    assert new_image.address.street == 'main st'
    assert new_image.address.zip_code == 12345

    keys = record.keys
    assert keys is not None
    assert keys.id == 'item-1'
    assert keys.version == 2


def test_modify_record():
    old_image = _image(name={'S': 'alice'}, count={'N': '41'})
    new_image = _image(name={'S': 'bob'}, count={'N': '42'})
    record = StreamModel.from_stream_record(
        _record('MODIFY', keys=KEYS, old_image=old_image, new_image=new_image))

    assert record.event_name == 'MODIFY'
    assert record.old_image is not None
    assert record.old_image.name == 'alice'
    assert record.old_image.count == 41
    assert record.new_image is not None
    assert record.new_image.name == 'bob'
    assert record.new_image.count == 42


def test_remove_record():
    record = StreamModel.from_stream_record(
        _record('REMOVE', keys=KEYS, old_image=_image(), StreamViewType='OLD_IMAGE'))

    assert record.event_name == 'REMOVE'
    assert record.new_image is None
    assert record.old_image is not None
    assert record.old_image.id == 'item-1'
    assert record.old_image.name == 'alice'
    assert record.old_image.payload == b'\x00\x01\x02'
    assert record.keys is not None
    assert record.keys.id == 'item-1'


def test_missing_image_is_none_not_default_instance():
    record = StreamModel.from_stream_record(_record('INSERT', keys=KEYS))

    assert record.new_image is None
    assert record.old_image is None
    assert record.keys is not None
    # the keys-only instance must not be filled with attribute defaults
    assert record.keys.name is None
    assert record.keys.count is None


def test_partial_image_does_not_apply_defaults():
    image = {'id': {'S': 'item-1'}, 'version': {'N': '2'}}
    record = StreamModel.from_stream_record(_record('INSERT', keys=KEYS, new_image=image))

    assert record.new_image is not None
    assert record.new_image.id == 'item-1'
    # 'name' declares a default, but it is absent from the image
    assert record.new_image.name is None
    assert record.new_image.address is None


def test_keys_only_view():
    record = StreamModel.from_stream_record(
        _record('REMOVE', keys=KEYS, StreamViewType='KEYS_ONLY'))

    assert record.stream_view_type == 'KEYS_ONLY'
    assert record.old_image is None
    assert record.new_image is None
    assert record.keys is not None
    assert record.keys.id == 'item-1'
    assert record.keys.version == 2
    assert record.keys.name is None


def test_unknown_attributes_dropped_by_default():
    image = _image(unknown_field={'S': 'mystery'}, another={'N': '1'})
    record = StreamModel.from_stream_record(_record('INSERT', keys=KEYS, new_image=image))

    assert record.new_image is not None
    assert not hasattr(record.new_image, 'unknown_field')
    assert not getattr(record.new_image, '_unknown_attributes', None)


def test_keep_unknown_attributes():
    image = _image(unknown_field={'S': 'mystery'}, another={'N': '1'})
    record = StreamModel.from_stream_record(
        _record('INSERT', keys=KEYS, new_image=image), keep_unknown_attributes=True)

    assert record.new_image is not None
    assert record.new_image._unknown_attributes == {
        'unknown_field': {'S': 'mystery'},
        'another': {'N': '1'},
    }
    # known attributes are not duplicated into the unknown mapping
    assert 'id' not in record.new_image._unknown_attributes


def test_deserialization_error_includes_image_and_path():
    image = _image(count={'S': 'not-a-number'})
    with pytest.raises(StreamRecordDeserializationError) as exc_info:
        StreamModel.from_stream_record(_record('INSERT', keys=KEYS, new_image=image))

    assert exc_info.value.image == 'new_image'
    assert exc_info.value.attr_path == 'count'
    assert 'new_image' in str(exc_info.value)
    assert 'count' in str(exc_info.value)


def test_deserialization_error_identifies_old_image():
    old_image = _image(count={'S': 'not-a-number'})
    with pytest.raises(StreamRecordDeserializationError) as exc_info:
        StreamModel.from_stream_record(
            _record('MODIFY', keys=KEYS, old_image=old_image, new_image=_image()))

    assert exc_info.value.image == 'old_image'
    assert exc_info.value.attr_path == 'count'


def test_deserialization_error_identifies_keys():
    keys = {'id': {'S': 'item-1'}, 'version': {'S': 'not-a-number'}}
    with pytest.raises(StreamRecordDeserializationError) as exc_info:
        StreamModel.from_stream_record(_record('REMOVE', keys=keys, old_image=_image()))

    assert exc_info.value.image == 'keys'
    assert exc_info.value.attr_path == 'version'


def test_nested_map_deserialization_error_path():
    image = _image(address={'M': {'street': {'S': 'main st'}, 'zip': {'S': 'not-a-number'}}})
    with pytest.raises(StreamRecordDeserializationError) as exc_info:
        StreamModel.from_stream_record(_record('INSERT', keys=KEYS, new_image=image))

    assert exc_info.value.image == 'new_image'
    assert exc_info.value.attr_path == 'address.zip_code'


def test_bare_stream_body_record():
    # a record consisting of only the "dynamodb" body is also accepted
    record = StreamModel.from_stream_record({
        'SequenceNumber': '1',
        'Keys': KEYS,
        'NewImage': _image(),
    })

    assert record.sequence_number == '1'
    assert record.event_name is None
    assert record.new_image is not None
    assert record.new_image.id == 'item-1'


def test_none_record_rejected():
    with pytest.raises(ValueError):
        StreamModel.from_stream_record(None)
