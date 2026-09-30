"""
Tests for partial projection loading-state tracking.

A model instance loaded with a projection (``attributes_to_get``) must not be
treated as if the item genuinely lacks the unprojected attributes: unloaded
attribute access raises a clear error, ``refresh_fields`` backfills specific
paths, ``save`` refuses to overwrite the whole item by default, and partial
``update`` keeps working for known fields.
"""
from unittest.mock import patch

import pytest

from pynamodb.attributes import (
    MapAttribute, NumberAttribute, UnicodeAttribute, VersionAttribute,
)
from pynamodb.exceptions import AttributeNotLoadedError
from pynamodb.models import Model

PATCH_METHOD = 'pynamodb.connection.Connection._make_api_call'


class OfficeMap(MapAttribute):
    office_id = UnicodeAttribute(null=True)
    city = UnicodeAttribute(null=True)


class Person(Model):
    class Meta:
        table_name = 'Person'

    user_id = UnicodeAttribute(hash_key=True)
    email = UnicodeAttribute(null=True)
    age = NumberAttribute(null=True)
    status = UnicodeAttribute(default='active')
    office = OfficeMap(null=True)


class VersionedPerson(Model):
    class Meta:
        table_name = 'VersionedPerson'

    user_id = UnicodeAttribute(hash_key=True)
    email = UnicodeAttribute(null=True)
    version = VersionAttribute()


class Document(Model):
    class Meta:
        table_name = 'Document'

    doc_id = UnicodeAttribute(hash_key=True)
    title = UnicodeAttribute(null=True)
    meta = MapAttribute(null=True)


FULL_ITEM = {
    'user_id': {'S': 'u1'},
    'email': {'S': 'u1@example.com'},
    'age': {'N': '30'},
    'status': {'S': 'remote-status'},
    'office': {'M': {'office_id': {'S': 'off-1'}, 'city': {'S': 'Lisbon'}}},
}

SPARSE_ITEM = {
    'user_id': {'S': 'u2'},
    'email': {'S': 'u2@example.com'},
}

VERSIONED_ITEM = {
    'user_id': {'S': 'u1'},
    'email': {'S': 'u1@example.com'},
    'version': {'N': '3'},
}


def _parse_projection(kwargs):
    expression = kwargs.get('ProjectionExpression')
    if not expression:
        return None
    names = kwargs.get('ExpressionAttributeNames', {})
    return [
        [names.get(segment, segment) for segment in part.split('.')]
        for part in expression.split(', ')
    ]


def _project(item, paths):
    result = {}
    for segments in paths:
        source, target = item, result
        for depth, segment in enumerate(segments):
            if segment not in source:
                break
            if depth == len(segments) - 1:
                target[segment] = source[segment]
            else:
                child = source[segment].get('M')
                if child is None:
                    break
                source = child
                target = target.setdefault(segment, {'M': {}})['M']
    return result


def _update_clause(expression, keyword):
    if keyword + ' ' not in expression:
        return ''
    rest = expression.split(keyword + ' ', 1)[1]
    for other in ('SET ', 'ADD ', 'REMOVE ', 'DELETE '):
        if ' ' + other in rest:
            rest = rest.split(' ' + other, 1)[0]
    return rest


class StubDynamoDB:
    """A minimal in-memory DynamoDB stub honoring projection expressions."""

    def __init__(self, items):
        self.items = items
        self.calls = []

    def __call__(self, operation_name, kwargs):
        self.calls.append((operation_name, kwargs))
        handler = getattr(self, 'on_' + operation_name.lower(), None)
        assert handler is not None, 'unexpected operation: {}'.format(operation_name)
        return handler(kwargs)

    def _key(self, kwargs):
        key = kwargs.get('Key') or kwargs['Item']
        return next(iter(key.values()))['S']

    def on_getitem(self, kwargs):
        item = self.items.get(self._key(kwargs))
        if item is None:
            return {}
        paths = _parse_projection(kwargs)
        return {'Item': _project(item, paths) if paths else dict(item)}

    def on_putitem(self, kwargs):
        item = dict(kwargs['Item'])
        self.items[self._key(kwargs)] = item
        return {}

    def on_updateitem(self, kwargs):
        item = self.items[self._key(kwargs)]
        names = kwargs.get('ExpressionAttributeNames', {})
        values = kwargs.get('ExpressionAttributeValues', {})
        expression = kwargs['UpdateExpression']
        for assignment in _update_clause(expression, 'SET').split(', '):
            if not assignment:
                continue
            name_placeholder, value_placeholder = assignment.split(' = ')
            item[names[name_placeholder]] = values[value_placeholder]
        for addition in _update_clause(expression, 'ADD').split(', '):
            if not addition:
                continue
            name_placeholder, value_placeholder = addition.split(' ')
            name = names[name_placeholder]
            current = int(item.get(name, {'N': '0'})['N'])
            item[name] = {'N': str(current + int(values[value_placeholder]['N']))}
        return {'Attributes': dict(item)}

    def on_scan(self, kwargs):
        paths = _parse_projection(kwargs)
        items = [_project(item, paths) if paths else dict(item) for item in self.items.values()]
        return {'Items': items, 'Count': len(items), 'ScannedCount': len(items)}


@pytest.fixture
def stub_db():
    db = StubDynamoDB({
        'u1': dict(FULL_ITEM),
        'u2': dict(SPARSE_ITEM),
    })
    with patch(PATCH_METHOD, new=db):
        yield db


@pytest.fixture
def versioned_db():
    db = StubDynamoDB({'u1': dict(VERSIONED_ITEM)})
    with patch(PATCH_METHOD, new=db):
        yield db


def test_get_with_projection_marks_unloaded_attributes(stub_db):
    item = Person.get('u1', attributes_to_get=['email'])

    assert item.user_id == 'u1'
    assert item.email == 'u1@example.com'
    assert item._is_partial()
    with pytest.raises(AttributeNotLoadedError, match="'age' is not loaded"):
        item.age
    with pytest.raises(AttributeNotLoadedError, match="'office' is not loaded"):
        item.office


def test_unprojected_attribute_with_default_is_not_defaulted(stub_db):
    item = Person.get('u1', attributes_to_get=['email'])

    with pytest.raises(AttributeNotLoadedError, match="'status' is not loaded"):
        item.status


def test_projected_missing_attribute_uses_default(stub_db):
    item = Person.get('u2', attributes_to_get=['email', 'status', 'age'])

    assert item.status == 'active'
    assert item.age is None
    with pytest.raises(AttributeNotLoadedError, match="'office' is not loaded"):
        item.office


def test_refresh_fields_backfills_attributes(stub_db):
    item = Person.get('u1', attributes_to_get=['email'])
    item.refresh_fields(['age', 'status'])

    assert item.age == 30
    assert item.status == 'remote-status'
    assert item._is_partial()
    with pytest.raises(AttributeNotLoadedError, match="'office' is not loaded"):
        item.office

    get_calls = [kwargs for op, kwargs in stub_db.calls if op == 'GetItem']
    assert get_calls[-1]['ProjectionExpression'] == '#0, #1'
    assert get_calls[-1]['ExpressionAttributeNames'] == {'#0': 'age', '#1': 'status'}


def test_nested_map_partial_projection_preserves_state(stub_db):
    item = Person.get('u1', attributes_to_get=['office.office_id'])

    assert item.office.office_id == 'off-1'
    with pytest.raises(AttributeNotLoadedError, match="'city' is not loaded"):
        item.office.city
    assert item._is_partial()

    item.refresh_fields(['office.city'])
    assert item.office.office_id == 'off-1'
    assert item.office.city == 'Lisbon'


def test_save_refused_for_partial_instance(stub_db):
    item = Person.get('u1', attributes_to_get=['email'])
    item.email = 'new@example.com'

    with pytest.raises(AttributeNotLoadedError, match='partial projection'):
        item.save()

    assert not [kwargs for op, kwargs in stub_db.calls if op == 'PutItem']
    assert stub_db.items['u1'] == FULL_ITEM


def test_save_allow_partial_writes_only_loaded_attributes(stub_db):
    item = Person.get('u1', attributes_to_get=['email'])
    item.email = 'new@example.com'
    item.save(allow_partial=True)

    put_calls = [kwargs for op, kwargs in stub_db.calls if op == 'PutItem']
    assert len(put_calls) == 1
    assert put_calls[0]['Item'] == {
        'user_id': {'S': 'u1'},
        'email': {'S': 'new@example.com'},
    }


def test_save_after_refresh_does_not_delete_remote_fields(stub_db):
    item = Person.get('u1', attributes_to_get=['email'])
    item.refresh()
    assert not item._is_partial()

    item.email = 'new@example.com'
    item.save()

    saved = stub_db.items['u1']
    assert saved['email'] == {'S': 'new@example.com'}
    assert saved['age'] == {'N': '30'}
    assert saved['status'] == {'S': 'remote-status'}
    assert saved['office'] == {'M': {'office_id': {'S': 'off-1'}, 'city': {'S': 'Lisbon'}}}


def test_save_after_refresh_fields_for_all_attributes(stub_db):
    item = Person.get('u1', attributes_to_get=['email'])
    item.refresh_fields(['age', 'status', 'office'])
    assert not item._is_partial()

    item.save()
    assert stub_db.items['u1']['status'] == {'S': 'remote-status'}


def test_update_works_on_partial_instance(stub_db):
    item = Person.get('u1', attributes_to_get=['email'])
    item.update([Person.email.set('updated@example.com')])

    update_calls = [kwargs for op, kwargs in stub_db.calls if op == 'UpdateItem']
    assert len(update_calls) == 1
    assert update_calls[0]['UpdateExpression'] == 'SET #0 = :0'
    assert update_calls[0]['ExpressionAttributeNames'] == {'#0': 'email'}

    assert not item._is_partial()
    assert item.email == 'updated@example.com'
    assert item.age == 30
    assert item.office.city == 'Lisbon'
    assert stub_db.items['u1']['email'] == {'S': 'updated@example.com'}


def test_scan_with_projection_marks_items_partial(stub_db):
    items = list(Person.scan(attributes_to_get=['email']))

    assert len(items) == 2
    assert all(item._is_partial() for item in items)
    with pytest.raises(AttributeNotLoadedError):
        items[0].age


def test_update_with_version_attribute_unloaded(versioned_db):
    item = VersionedPerson.get('u1', attributes_to_get=['email'])
    item.update([VersionedPerson.email.set('updated@example.com')])

    update_calls = [kwargs for op, kwargs in versioned_db.calls if op == 'UpdateItem']
    assert len(update_calls) == 1
    assert 'ConditionExpression' not in update_calls[0]
    assert item.version == 4


def test_update_with_version_attribute_loaded(versioned_db):
    item = VersionedPerson.get('u1', attributes_to_get=['email', 'version'])
    assert item.version == 3
    item.update([VersionedPerson.email.set('updated@example.com')])

    update_calls = [kwargs for op, kwargs in versioned_db.calls if op == 'UpdateItem']
    assert len(update_calls) == 1
    assert 'ConditionExpression' in update_calls[0]
    assert item.version == 4


def test_full_get_is_not_partial(stub_db):
    item = Person.get('u1')

    assert not item._is_partial()
    assert item.age == 30
    assert item.office.city == 'Lisbon'
    item.save()
    assert stub_db.items['u1'] == FULL_ITEM


def test_raw_map_partial_projection_preserves_state():
    db = StubDynamoDB({
        'd1': {
            'doc_id': {'S': 'd1'},
            'title': {'S': 'report'},
            'meta': {'M': {'x': {'S': '1'}, 'y': {'S': '2'}}},
        },
    })
    with patch(PATCH_METHOD, new=db):
        item = Document.get('d1', attributes_to_get=['meta.x'])

        assert item.meta['x'] == '1'
        with pytest.raises(AttributeNotLoadedError, match="'y' is not loaded"):
            item.meta.y
        with pytest.raises(AttributeNotLoadedError, match="'y' is not loaded"):
            item.meta['y']
        assert item._is_partial()

        item.refresh_fields(['meta.y'])
        assert item.meta['x'] == '1'
        assert item.meta['y'] == '2'
        with pytest.raises(AttributeNotLoadedError, match="'title' is not loaded"):
            item.title
