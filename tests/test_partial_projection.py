"""
Tests for loading-state tracking of models loaded with partial projections.
"""
from unittest.mock import patch

import pytest

from pynamodb.attributes import MapAttribute, NumberAttribute, UnicodeAttribute
from pynamodb.exceptions import AttributeNotLoadedError, InvalidStateError
from pynamodb.expressions.projection import get_projection_document_paths
from pynamodb.expressions.update import RemoveAction, SetAction
from pynamodb.models import Model


class PersonMap(MapAttribute):
    name = UnicodeAttribute()
    age = NumberAttribute()


class ProjectedModel(Model):
    class Meta:
        table_name = 'ProjectedModel'
        host = 'http://localhost:8000'

    user_id = UnicodeAttribute(hash_key=True)
    email = UnicodeAttribute()
    status = UnicodeAttribute(default='active')
    views = NumberAttribute()
    person = PersonMap()
    raw_map = MapAttribute()


class CustomAttrModel(Model):
    class Meta:
        table_name = 'CustomAttrModel'
        host = 'http://localhost:8000'

    user_id = UnicodeAttribute(hash_key=True)
    email = UnicodeAttribute(attr_name='custom_email')
    views = NumberAttribute()


FULL_ITEM = {
    'user_id': {'S': 'user-1'},
    'email': {'S': 'user@example.com'},
    'status': {'S': 'suspended'},
    'views': {'N': '42'},
    'person': {'M': {'name': {'S': 'Alice'}, 'age': {'N': '30'}}},
    'raw_map': {'M': {'foo': {'S': 'bar'}, 'secret': {'S': 'shh'}}},
}


def _project_item(item, attributes_to_get):
    """Returns a copy of `item` containing only the projected document paths."""
    result = {}
    for segments in get_projection_document_paths(attributes_to_get):
        source, target = item, result
        for segment in segments[:-1]:
            nested = source.get(segment, {})
            if 'M' not in nested:
                break
            source = nested['M']
            target = target.setdefault(segment, {'M': {}})['M']
        else:
            if segments[-1] in source:
                target[segments[-1]] = source[segments[-1]]
    return result


def _set_path(item, segments, value):
    for segment in segments[:-1]:
        item = item.setdefault(segment, {'M': {}})['M']
    item[segments[-1]] = value


def _remove_path(item, segments):
    for segment in segments[:-1]:
        item = item.get(segment, {}).get('M', {})
    item.pop(segments[-1], None)


class StubConnection:
    """
    An in-memory stub of `pynamodb.connection.table.TableConnection` holding a single item.
    """

    def __init__(self, item):
        self.item = item
        self.get_item_calls = []
        self.put_item_calls = []

    def get_item(self, hash_key, range_key=None, consistent_read=None, attributes_to_get=None):
        self.get_item_calls.append(attributes_to_get)
        item = self.item
        if attributes_to_get is not None:
            item = _project_item(item, attributes_to_get)
        return {'Item': item}

    def put_item(self, hash_key, range_key=None, attributes=None, **kwargs):
        self.put_item_calls.append(attributes)
        # PutItem overwrites the entire item.
        self.item = {'user_id': {'S': hash_key}, **(attributes or {})}
        return {}

    def update_item(self, hash_key, range_key=None, actions=None, return_values=None, **kwargs):
        for action in actions or []:
            if isinstance(action, SetAction):
                path, value = action.values
                _set_path(self.item, path.path, value.value)
            elif isinstance(action, RemoveAction):
                (path,) = action.values
                _remove_path(self.item, path.path)
        return {'Attributes': self.item}

    def scan(self, **kwargs):
        attributes_to_get = kwargs.get('attributes_to_get')
        item = self.item if attributes_to_get is None else _project_item(self.item, attributes_to_get)
        return {'Items': [item], 'Count': 1, 'ScannedCount': 1}

    def batch_get_item(self, keys_to_get, consistent_read=None, attributes_to_get=None, **kwargs):
        item = self.item if attributes_to_get is None else _project_item(self.item, attributes_to_get)
        return {'Responses': {'ProjectedModel': [item]}, 'UnprocessedKeys': {}}

    def batch_write_item(self, put_items=None, delete_items=None, **kwargs):
        self.batch_write_calls = getattr(self, 'batch_write_calls', [])
        self.batch_write_calls.append({'put_items': put_items, 'delete_items': delete_items})
        return {}


@pytest.fixture
def connection():
    connection = StubConnection({key: dict(value) for key, value in FULL_ITEM.items()})
    with patch.object(ProjectedModel, '_get_connection', return_value=connection):
        yield connection


def test_full_get_is_fully_loaded(connection):
    item = ProjectedModel.get('user-1')
    assert item._loaded_attributes is None
    assert item.email == 'user@example.com'
    assert item.views == 42
    assert item.person.name == 'Alice'
    item.save()
    assert len(connection.put_item_calls) == 1


def test_projection_tracks_loaded_attributes(connection):
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'email'])
    assert item.user_id == 'user-1'
    assert item.email == 'user@example.com'
    with pytest.raises(AttributeNotLoadedError):
        item.views
    with pytest.raises(AttributeNotLoadedError):
        item.person


def test_projection_does_not_apply_defaults_to_unloaded_attributes(connection):
    # `status` has a default of 'active' but was not projected: its remote value is unknown.
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'email'])
    with pytest.raises(AttributeNotLoadedError):
        item.status


def test_projection_applies_default_only_for_confirmed_absent(connection):
    # The projected attribute exists remotely: the remote value wins over the default.
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'status'])
    assert item.status == 'suspended'

    # The projected attribute does not exist remotely: the default applies.
    connection.item = {key: value for key, value in connection.item.items() if key != 'status'}
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'status'])
    assert item.status == 'active'


def test_nested_map_partial_projection(connection):
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'person.name'])
    assert item.person.name == 'Alice'
    with pytest.raises(AttributeNotLoadedError):
        item.person.age


def test_raw_map_partial_projection(connection):
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'raw_map.foo'])
    assert item.raw_map.foo == 'bar'
    assert item.raw_map['foo'] == 'bar'
    with pytest.raises(AttributeNotLoadedError):
        item.raw_map.secret
    with pytest.raises(AttributeNotLoadedError):
        item.raw_map['secret']


def test_scan_projection_tracks_loaded_attributes(connection):
    (item,) = list(ProjectedModel.scan(attributes_to_get=['user_id', 'email']))
    assert item.email == 'user@example.com'
    with pytest.raises(AttributeNotLoadedError):
        item.views


def test_batch_get_projection_tracks_loaded_attributes(connection):
    (item,) = list(ProjectedModel.batch_get(['user-1'], attributes_to_get=['user_id', 'email']))
    assert item.email == 'user@example.com'
    with pytest.raises(AttributeNotLoadedError):
        item.views


def test_refresh_fields_backfills_attributes(connection):
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'email'])
    with pytest.raises(AttributeNotLoadedError):
        item.views

    item.refresh_fields('views')
    assert item.views == 42
    # Other attributes remain unloaded.
    with pytest.raises(AttributeNotLoadedError):
        item.status


def test_refresh_fields_merges_nested_map_state(connection):
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'person.name'])
    item.refresh_fields('person.age')
    assert item.person.name == 'Alice'
    assert item.person.age == 30
    with pytest.raises(AttributeNotLoadedError):
        item.views


def test_refresh_fields_merges_raw_map_state(connection):
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'raw_map.foo'])
    item.refresh_fields('raw_map.secret')
    assert item.raw_map.foo == 'bar'
    assert item.raw_map.secret == 'shh'


def test_refresh_fields_accepts_attribute_and_list(connection):
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id'])
    item.refresh_fields(ProjectedModel.email, ['views'])
    assert item.email == 'user@example.com'
    assert item.views == 42
    assert connection.get_item_calls[-1] == [ProjectedModel.email, 'views']


def test_refresh_makes_instance_fully_loaded(connection):
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'email'])
    item.refresh()
    assert item._loaded_attributes is None
    assert item.views == 42
    assert item.person.age == 30


def test_save_refuses_partial_instance(connection):
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'email'])
    item.email = 'new@example.com'
    with pytest.raises(InvalidStateError):
        item.save()
    # The remote item was not overwritten: unread attributes were not deleted.
    assert connection.put_item_calls == []
    assert connection.item == FULL_ITEM


def test_save_allowed_after_refresh_fields_cover_everything(connection):
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'email'])
    item.refresh_fields('status', 'views', 'person', 'raw_map')
    item.save()
    assert len(connection.put_item_calls) == 1
    assert connection.item == FULL_ITEM


def test_save_allowed_after_refresh(connection):
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'email'])
    item.refresh()
    item.save()
    assert len(connection.put_item_calls) == 1
    assert connection.item == FULL_ITEM


def test_allow_partial_save_override(connection):
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'email'])
    item.save(allow_partial_save=True)
    assert len(connection.put_item_calls) == 1
    # The explicit opt-in overwrites the item with only the loaded attributes.
    assert connection.item == {'user_id': {'S': 'user-1'}, 'email': {'S': 'user@example.com'}}


def test_update_on_partial_instance_preserves_unread_remote_fields(connection):
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'views'])
    item.update([ProjectedModel.views.set(43)])

    # The partial update only touched `views`; attributes that were never read are preserved.
    assert connection.item['views'] == {'N': '43'}
    assert connection.item['email'] == {'S': 'user@example.com'}
    assert connection.item['raw_map'] == {'M': {'foo': {'S': 'bar'}, 'secret': {'S': 'shh'}}}

    # UpdateItem returns the entire item, so the instance is fully loaded afterwards.
    assert item._loaded_attributes is None
    assert item.email == 'user@example.com'
    item.save()
    assert len(connection.put_item_calls) == 1
    assert connection.item['raw_map'] == {'M': {'foo': {'S': 'bar'}, 'secret': {'S': 'shh'}}}


def test_setting_an_attribute_marks_it_loaded(connection):
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'email'])
    item.views = 7
    assert item.views == 7
    # Attributes that were neither loaded nor set are still guarded.
    with pytest.raises(AttributeNotLoadedError):
        item.status


def test_batch_write_refuses_partial_instance(connection):
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'email'])
    with pytest.raises(InvalidStateError):
        with ProjectedModel.batch_write() as batch:
            batch.save(item)
    # Nothing was written: unread remote attributes were not deleted.
    assert connection.item == FULL_ITEM


def test_transaction_save_kwargs_refuse_partial_instance(connection):
    item = ProjectedModel.get('user-1', attributes_to_get=['user_id', 'email'])
    with pytest.raises(InvalidStateError):
        item.get_save_kwargs_from_instance()


def test_projection_with_custom_attr_name():
    connection = StubConnection({
        'user_id': {'S': 'user-1'},
        'custom_email': {'S': 'user@example.com'},
        'views': {'N': '42'},
    })
    with patch.object(CustomAttrModel, '_get_connection', return_value=connection):
        item = CustomAttrModel.get('user-1', attributes_to_get=['user_id', 'custom_email'])
        assert item.email == 'user@example.com'
        with pytest.raises(AttributeNotLoadedError):
            item.views
        item.refresh_fields('views')
        assert item.views == 42
