from base64 import b64encode
from datetime import datetime
from datetime import timezone

import pytest

from pynamodb.attributes import BinaryAttribute
from pynamodb.attributes import BinarySetAttribute
from pynamodb.attributes import DynamicMapAttribute
from pynamodb.attributes import DiscriminatorAttribute
from pynamodb.attributes import ListAttribute
from pynamodb.attributes import MapAttribute
from pynamodb.attributes import NumberSetAttribute
from pynamodb.attributes import UnicodeAttribute
from pynamodb.attributes import UnicodeSetAttribute
from pynamodb.exceptions import AttributeDeserializationError
from pynamodb.exceptions import StreamAttributeDeserializationError
from pynamodb.models import Model
from pynamodb.streams import INSERT
from pynamodb.streams import MODIFY
from pynamodb.streams import REMOVE


class Address(MapAttribute):
    city = UnicodeAttribute()
    zip_code = UnicodeAttribute(attr_name='zip')


class Profile(DynamicMapAttribute):
    name = UnicodeAttribute()


class StreamItem(Model):
    class Meta:
        table_name = 'stream-items'

    pk = UnicodeAttribute(hash_key=True)
    sk = UnicodeAttribute(range_key=True)
    description = UnicodeAttribute(null=True, default='loaded default')
    data = BinaryAttribute(null=True, legacy_encoding=False)
    tags = UnicodeSetAttribute(null=True)
    numbers = NumberSetAttribute(null=True)
    blobs = BinarySetAttribute(null=True, legacy_encoding=False)
    address = Address(null=True)
    profile = Profile(null=True)
    raw_values = MapAttribute(null=True)
    raw_items = ListAttribute(null=True)


class PolymorphicStreamItem(Model):
    class Meta:
        table_name = 'polymorphic-stream-items'

    pk = UnicodeAttribute(hash_key=True)
    cls = DiscriminatorAttribute()


class PolymorphicChild(PolymorphicStreamItem, discriminator='child'):
    name = UnicodeAttribute(null=True)


def make_record(event_name, *, old_image=None, new_image=None, keys=None, **dynamodb):
    stream_payload = {
        'Keys': keys or {'pk': {'S': 'partition'}, 'sk': {'S': 'sort'}},
        'SequenceNumber': '1007',
        'SizeBytes': 42,
        'StreamViewType': 'NEW_AND_OLD_IMAGES',
        'ApproximateCreationDateTime': datetime(2020, 1, 1, tzinfo=timezone.utc),
    }
    if old_image is not None:
        stream_payload['OldImage'] = old_image
    if new_image is not None:
        stream_payload['NewImage'] = new_image
    stream_payload.update(dynamodb)

    return {
        'eventID': 'event-id',
        'eventName': event_name,
        'eventVersion': '1.1',
        'eventSource': 'aws:dynamodb',
        'awsRegion': 'us-east-1',
        'dynamodb': {
            **stream_payload,
        },
    }


def test_insert_decodes_new_image_and_primary_key_without_old_image():
    record = make_record(
        INSERT,
        new_image={
            'pk': {'S': 'partition'},
            'sk': {'S': 'sort'},
            'description': {'S': 'created'},
            'data': {'B': b64encode(b'binary').decode()},
            'tags': {'SS': ['red', 'blue']},
            'numbers': {'NS': ['1', '2']},
            'blobs': {'BS': [b64encode(b'one').decode(), b64encode(b'two').decode()]},
            'address': {'M': {'city': {'S': 'Portland'}, 'zip': {'S': '97201'}}},
            'profile': {'M': {'name': {'S': 'Ada'}, 'title': {'S': 'engineer'}}},
        },
    )

    result = StreamItem.from_stream_record(record)

    assert result.event_name == INSERT
    assert result.old_image is None
    assert result.sequence_number == '1007'
    assert result.size_bytes == 42
    assert result.stream_view_type == 'NEW_AND_OLD_IMAGES'
    assert result.approximate_creation_date_time == datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert result.event_id == 'event-id'
    assert result.event_version == '1.1'
    assert result.event_source == 'aws:dynamodb'
    assert result.aws_region == 'us-east-1'
    assert result.dynamodb is record['dynamodb']
    assert result.record is record

    assert result.key.attribute_values == {'pk': 'partition', 'sk': 'sort'}
    assert result.new_image.pk == 'partition'
    assert result.new_image.description == 'created'
    assert result.new_image.data == b'binary'
    assert result.new_image.tags == {'red', 'blue'}
    assert result.new_image.numbers == {1, 2}
    assert result.new_image.blobs == {b'one', b'two'}
    assert result.new_image.address.city == 'Portland'
    assert result.new_image.address.zip_code == '97201'
    assert result.new_image.profile.name == 'Ada'
    assert result.new_image.profile.title == 'engineer'


def test_modify_decodes_old_and_new_images():
    record = make_record(
        MODIFY,
        old_image={
            'pk': {'S': 'partition'},
            'sk': {'S': 'sort'},
            'description': {'S': 'before'},
        },
        new_image={
            'pk': {'S': 'partition'},
            'sk': {'S': 'sort'},
            'description': {'S': 'after'},
        },
    )

    result = StreamItem.from_stream_record(record)

    assert result.event_name == MODIFY
    assert result.old_image.description == 'before'
    assert result.new_image.description == 'after'
    assert result.key.pk == 'partition'
    assert result.key.sk == 'sort'


def test_remove_decodes_old_image_without_new_image():
    record = make_record(
        REMOVE,
        old_image={
            'pk': {'S': 'partition'},
            'sk': {'S': 'sort'},
            'description': {'S': 'deleted'},
            'data': {'B': b'binary'},
            'blobs': {'BS': [b'one']},
        },
    )

    result = StreamItem.from_stream_record(record)

    assert result.event_name == REMOVE
    assert result.new_image is None
    assert result.old_image.description == 'deleted'
    assert result.old_image.data == b'binary'
    assert result.old_image.blobs == {b'one'}


def test_partial_images_do_not_get_default_values():
    record = make_record(
        INSERT,
        keys={'pk': {'S': 'partition'}, 'sk': {'S': 'sort'}},
        new_image={'pk': {'S': 'partition'}, 'sk': {'S': 'sort'}},
    )

    result = StreamItem.from_stream_record(record)

    assert 'description' not in result.key.attribute_values
    assert result.key.description is None
    assert 'description' not in result.new_image.attribute_values
    assert result.new_image.description is None


def test_keys_only_stream_image_does_not_get_other_model_fields():
    record = make_record(
        MODIFY,
        keys={'pk': {'S': 'partition'}, 'sk': {'S': 'sort'}},
        StreamViewType='KEYS_ONLY',
    )

    result = StreamItem.from_stream_record(record)

    assert result.old_image is None
    assert result.new_image is None
    assert result.key.attribute_values == {'pk': 'partition', 'sk': 'sort'}
    assert result.stream_view_type == 'KEYS_ONLY'


def test_raw_maps_and_lists_decode_nested_values():
    record = make_record(INSERT, new_image={
        'pk': {'S': 'partition'},
        'sk': {'S': 'sort'},
        'raw_values': {'M': {
            'binary': {'B': b64encode(b'nested').decode()},
            'nested': {'M': {'numbers': {'NS': ['3', '4']}}},
        }},
        'raw_items': {'L': [
            {'S': 'first'},
            {'M': {'binary': {'B': b64encode(b'item').decode()}}},
        ]},
    })

    result = StreamItem.from_stream_record(record)

    assert result.new_image.raw_values.as_dict() == {
        'binary': b'nested',
        'nested': {'numbers': {3, 4}},
    }
    assert result.new_image.raw_items == [
        'first',
        {'binary': b'item'},
    ]


def test_polymorphic_key_image_does_not_invent_discriminator_value():
    record = make_record(
        MODIFY,
        keys={'pk': {'S': 'partition'}},
        old_image={'pk': {'S': 'partition'}, 'cls': {'S': 'child'}, 'name': {'S': 'Ada'}},
        new_image={'pk': {'S': 'partition'}, 'cls': {'S': 'child'}, 'name': {'S': 'Grace'}},
    )

    result = PolymorphicStreamItem.from_stream_record(record)

    assert type(result.key) is PolymorphicStreamItem
    assert result.key.attribute_values == {'pk': 'partition'}
    assert isinstance(result.old_image, PolymorphicChild)
    assert result.old_image.name == 'Ada'
    assert isinstance(result.new_image, PolymorphicChild)
    assert result.new_image.name == 'Grace'


def test_preserve_unknown_attributes_keeps_removed_model_fields():
    record = make_record(
        INSERT,
        new_image={
            'pk': {'S': 'partition'},
            'sk': {'S': 'sort'},
            'removed_top_level': {'S': 'kept'},
            'address': {'M': {
                'city': {'S': 'Portland'},
                'former_street': {'S': 'Main Street'},
            }},
        },
    )

    result = StreamItem.from_stream_record(record, preserve_unknown_attributes=True)

    assert result.new_image.removed_top_level == 'kept'
    assert result.new_image.address.former_street == 'Main Street'


def test_unknown_attributes_are_not_retained_by_default():
    record = make_record(
        INSERT,
        new_image={
            'pk': {'S': 'partition'},
            'sk': {'S': 'sort'},
            'removed_top_level': {'S': 'ignored'},
            'address': {'M': {
                'city': {'S': 'Portland'},
                'former_street': {'S': 'Main Street'},
            }},
        },
    )

    result = StreamItem.from_stream_record(record)

    assert 'removed_top_level' not in result.new_image.attribute_values
    assert 'former_street' not in result.new_image.address.attribute_values
    with pytest.raises(AttributeError):
        result.new_image.removed_top_level


def test_deserialization_failure_includes_image_and_nested_field_path():
    new_image = {
        'pk': {'S': 'partition'},
        'sk': {'S': 'sort'},
        'address': {'M': {'city': {'N': 'invalid'}}},
    }
    record = make_record(INSERT, new_image=new_image)

    with pytest.raises(StreamAttributeDeserializationError) as exc_info:
        StreamItem.from_stream_record(record)

    error = exc_info.value
    assert error.image is new_image
    assert error.field_path == 'address.city'
    assert error.image_name == 'NewImage'
    assert isinstance(error.__cause__, AttributeDeserializationError)


def test_invalid_event_name_is_rejected():
    record = make_record('DELETE')

    with pytest.raises(ValueError, match='eventName'):
        StreamItem.from_stream_record(record)


def test_missing_dynamodb_mapping_is_rejected():
    with pytest.raises(ValueError, match='dynamodb'):
        StreamItem.from_stream_record({'eventName': INSERT})
