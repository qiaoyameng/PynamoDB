import pytest
from pynamodb.attributes import MapAttribute, NumberAttribute, UnicodeAttribute, VersionAttribute

from pynamodb.connection import Connection
from pynamodb.connection.base import MetaTable
from pynamodb.constants import TABLE_KEY
from pynamodb.exceptions import TransactWriteMergeError
from pynamodb.transactions import Transaction, TransactGet, TransactWrite
from pynamodb.models import Model
from tests.test_base_connection import PATCH_METHOD

from unittest.mock import patch


class MockModel(Model):
    class Meta:
        table_name = 'mock'

    mock_hash = NumberAttribute(hash_key=True)
    mock_range = NumberAttribute(range_key=True)
    mock_toot = UnicodeAttribute(null=True)
    mock_version = VersionAttribute()


class MockAddress(MapAttribute):
    city = UnicodeAttribute(null=True)
    zip_code = UnicodeAttribute(null=True, attr_name='z')


class MergeModel(Model):
    class Meta:
        table_name = 'mock'

    mock_hash = NumberAttribute(hash_key=True)
    mock_range = NumberAttribute(range_key=True)
    mock_toot = UnicodeAttribute(null=True)
    mock_other = UnicodeAttribute(null=True)
    aliased = UnicodeAttribute(null=True, attr_name='al')
    address = MockAddress(null=True)
    mock_version = VersionAttribute()


class OtherTableModel(MergeModel):
    class Meta:
        table_name = 'other'


MOCK_TABLE_DESCRIPTOR = {
    "Table": {
        "TableName": "mock",
        "KeySchema": [
            {
                "AttributeName": "mock_hash",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "mock_range",
                "KeyType": "RANGE"
            }
        ],
        "AttributeDefinitions": [
            {
                "AttributeName": "mock_hash",
                "AttributeType": "N"
            },
            {
                "AttributeName": "mock_range",
                "AttributeType": "N"
            }
        ]
    }
}


class TestTransaction:

    def test_commit__not_implemented(self):
        t = Transaction(connection=Connection())
        with pytest.raises(NotImplementedError):
            t._commit()


class TestTransactGet:

    def test_commit(self, mocker):
        connection = Connection()
        connection.add_meta_table(MetaTable(MOCK_TABLE_DESCRIPTOR[TABLE_KEY]))

        mock_connection_transact_get = mocker.patch.object(connection, 'transact_get_items')

        with TransactGet(connection=connection) as t:
            t.get(MockModel, 1, 2)

        mock_connection_transact_get.assert_called_once_with(
            get_items=[{'Key': {'mock_hash': {'N': '1'}, 'mock_range': {'N': '2'}}, 'TableName': 'mock'}],
            return_consumed_capacity=None
        )


class TestTransactWrite:

    def test_condition_check__no_condition(self):
        with pytest.raises(TypeError):
            with TransactWrite(connection=Connection()) as transaction:
                transaction.condition_check(MockModel, hash_key=1, condition=None)

    def test_commit(self, mocker):
        connection = Connection()
        mock_connection_transact_write = mocker.patch.object(connection, 'transact_write_items')
        with TransactWrite(connection=connection) as t:
            t.condition_check(MockModel, 1, 3, condition=(MockModel.mock_hash.does_not_exist()))
            t.delete(MockModel(2, 4))
            t.save(MockModel(3, 5))
            t.update(MockModel(4, 6), actions=[MockModel.mock_toot.set('hello')], return_values='ALL_OLD')

        expected_condition_checks = [{
            'ConditionExpression': 'attribute_not_exists (#0)',
            'ExpressionAttributeNames': {'#0': 'mock_hash'},
            'Key': {'mock_hash': {'N': '1'}, 'mock_range': {'N': '3'}},
            'TableName': 'mock'}
        ]
        expected_deletes = [{
            'ConditionExpression': 'attribute_not_exists (#0)',
            'ExpressionAttributeNames': {'#0': 'mock_version'},
            'Key': {'mock_hash': {'N': '2'}, 'mock_range': {'N': '4'}},
            'TableName': 'mock'
        }]
        expected_puts = [{
            'ConditionExpression': 'attribute_not_exists (#0)',
            'ExpressionAttributeNames': {'#0': 'mock_version'},
            'Item': {'mock_hash': {'N': '3'}, 'mock_range': {'N': '5'}, 'mock_version': {'N': '1'}},
            'TableName': 'mock'
        }]
        expected_updates = [{
            'ConditionExpression': 'attribute_not_exists (#0)',
            'TableName': 'mock',
            'Key': {'mock_hash': {'N': '4'}, 'mock_range': {'N': '6'}},
            'ReturnValuesOnConditionCheckFailure': 'ALL_OLD',
            'UpdateExpression': 'SET #1 = :0, #0 = :1',
            'ExpressionAttributeNames': {'#0': 'mock_version', '#1': 'mock_toot'},
            'ExpressionAttributeValues': {':0': {'S': 'hello'}, ':1': {'N': '1'}}
        }]
        mock_connection_transact_write.assert_called_once_with(
            condition_check_items=expected_condition_checks,
            delete_items=expected_deletes,
            put_items=expected_puts,
            update_items=expected_updates,
            client_request_token=None,
            return_consumed_capacity=None,
            return_item_collection_metrics=None
        )


class TestTransactWriteMerge:
    """Tests for the optional merge (coalescing) phase."""

    def _write(self, mocker, populate, **tx_kwargs):
        connection = Connection()
        mock_transact_write = mocker.patch.object(connection, 'transact_write_items')
        with TransactWrite(connection=connection, merge=True, **tx_kwargs) as transaction:
            populate(transaction)
        return mock_transact_write.call_args.kwargs

    def test_merge_disabled_by_default(self, mocker):
        connection = Connection()
        mock_transact_write = mocker.patch.object(connection, 'transact_write_items')
        with TransactWrite(connection=connection) as transaction:
            transaction.update(MockModel(1, 2), actions=[MockModel.mock_toot.set('a')])
            transaction.update(MockModel(1, 2), actions=[MockModel.mock_toot.set('b')])
        # without merging, both updates are sent as-is (DynamoDB would reject)
        assert len(mock_transact_write.call_args.kwargs['update_items']) == 2

    def test_merge_non_conflicting_updates(self, mocker):
        def populate(transaction):
            transaction.update(MergeModel(1, 2), actions=[MergeModel.mock_toot.set('hello')],
                               condition=MergeModel.mock_toot.does_not_exist())
            transaction.update(MergeModel(1, 2), actions=[MergeModel.mock_other.set('world')])

        kwargs = self._write(mocker, populate)
        assert kwargs['update_items'] and not kwargs['condition_check_items']
        update = kwargs['update_items'][0]
        names = update['ExpressionAttributeNames']
        values = update['ExpressionAttributeValues']
        field_names = set(names.values())
        assert {'mock_toot', 'mock_other', 'mock_version'} <= field_names
        # exactly one version increment and one version condition
        assert update['UpdateExpression'].count('mock_version') == 0
        version_placeholders = [ph for ph, name in names.items() if name == 'mock_version']
        assert len(version_placeholders) == 1
        assert update['UpdateExpression'].count(version_placeholders[0]) == 1
        assert update['ConditionExpression'].count(version_placeholders[0]) == 1
        # both user values are present
        assert {'S': 'hello'} in values.values()
        assert {'S': 'world'} in values.values()

    def test_identical_actions_deduplicated(self, mocker):
        def populate(transaction):
            action = MergeModel.mock_toot.set('hello')
            transaction.update(MergeModel(1, 2), actions=[action])
            transaction.update(MergeModel(1, 2), actions=[MergeModel.mock_toot.set('hello')])

        kwargs = self._write(mocker, populate)
        update = kwargs['update_items'][0]
        assert update['UpdateExpression'].count('=') == 2  # user SET + version SET

    def test_conflicting_field_updates_rejected(self, mocker):
        def populate(transaction):
            transaction.update(MergeModel(1, 2), actions=[MergeModel.mock_toot.set('a')])
            transaction.update(MergeModel(1, 2), actions=[MergeModel.mock_toot.set('b')])

        with pytest.raises(TransactWriteMergeError) as exc_info:
            self._write(mocker, populate)
        error = exc_info.value
        assert len(error.actions) == 2
        assert error.actions[0].operation == 'update'
        assert error.actions[0].filename.endswith('test_transaction.py')
        assert error.actions[0].lineno < error.actions[1].lineno

    def test_parent_child_path_conflict_rejected(self, mocker):
        def populate(transaction):
            transaction.update(MergeModel(1, 2), actions=[MergeModel.address.set({'city': 'X'})])
            transaction.update(MergeModel(1, 2), actions=[MergeModel.address.city.set('Y')])

        with pytest.raises(TransactWriteMergeError) as exc_info:
            self._write(mocker, populate)
        assert 'document path' in exc_info.value.msg

    def test_sibling_nested_fields_merge(self, mocker):
        def populate(transaction):
            transaction.update(MergeModel(1, 2), actions=[MergeModel.address.city.set('Y')])
            transaction.update(MergeModel(1, 2), actions=[MergeModel.address.zip_code.set('Z')])

        kwargs = self._write(mocker, populate)
        update = kwargs['update_items'][0]
        names = update['ExpressionAttributeNames']
        assert 'city' in names.values() and 'z' in names.values()

    def test_alias_normalized_before_merge(self, mocker):
        # the python name differs from attr_name, but both actions target 'al'
        def populate(transaction):
            transaction.update(MergeModel(1, 2), actions=[MergeModel.aliased.set('a')])
            transaction.update(MergeModel(1, 2), actions=[MergeModel.aliased.set('b')])

        with pytest.raises(TransactWriteMergeError):
            self._write(mocker, populate)

    def test_different_composite_key_items_not_merged(self, mocker):
        def populate(transaction):
            transaction.update(MergeModel(1, 2), actions=[MergeModel.mock_toot.set('a')])
            transaction.update(MergeModel(1, 3), actions=[MergeModel.mock_toot.set('b')])

        kwargs = self._write(mocker, populate)
        assert len(kwargs['update_items']) == 2

    def test_different_tables_same_key_not_merged(self, mocker):
        def populate(transaction):
            transaction.update(MergeModel(1, 2), actions=[MergeModel.mock_toot.set('a')])
            transaction.update(OtherTableModel(1, 2), actions=[OtherTableModel.mock_toot.set('a')])

        kwargs = self._write(mocker, populate)
        tables = {item['TableName'] for item in kwargs['update_items']}
        assert tables == {'mock', 'other'}

    def test_delete_and_update_rejected(self, mocker):
        def populate(transaction):
            transaction.update(MergeModel(1, 2), actions=[MergeModel.mock_toot.set('a')])
            transaction.delete(MergeModel(1, 2))

        with pytest.raises(TransactWriteMergeError) as exc_info:
            self._write(mocker, populate)
        assert "'delete'" in exc_info.value.msg and "'update'" in exc_info.value.msg

    def test_put_and_update_rejected(self, mocker):
        def populate(transaction):
            transaction.save(MergeModel(1, 2))
            transaction.update(MergeModel(1, 2), actions=[MergeModel.mock_toot.set('a')])

        with pytest.raises(TransactWriteMergeError):
            self._write(mocker, populate)

    def test_delete_merged_with_and_conditions(self, mocker):
        def populate(transaction):
            transaction.delete(MergeModel(1, 2), condition=MergeModel.mock_toot.exists())
            transaction.delete(MergeModel(1, 2), condition=MergeModel.mock_other.exists())

        kwargs = self._write(mocker, populate)
        assert len(kwargs['delete_items']) == 1
        expression = kwargs['delete_items'][0]['ConditionExpression']
        assert 'AND' in expression

    def test_condition_check_folded_into_update(self, mocker):
        def populate(transaction):
            transaction.condition_check(MergeModel, 1, 2, condition=MergeModel.mock_other.exists())
            transaction.update(MergeModel(1, 2), actions=[MergeModel.mock_toot.set('a')],
                               condition=MergeModel.mock_toot.does_not_exist())

        kwargs = self._write(mocker, populate)
        assert kwargs['condition_check_items'] == []
        assert len(kwargs['update_items']) == 1
        expression = kwargs['update_items'][0]['ConditionExpression']
        assert 'AND' in expression

    def test_condition_checks_on_same_item_merged(self, mocker):
        def populate(transaction):
            transaction.condition_check(MergeModel, 1, 2, condition=MergeModel.mock_toot.exists())
            transaction.condition_check(MergeModel, 1, 2, condition=MergeModel.mock_other.exists())

        kwargs = self._write(mocker, populate)
        assert len(kwargs['condition_check_items']) == 1
        assert 'AND' in kwargs['condition_check_items'][0]['ConditionExpression']

    def test_condition_check_and_delete_conflict(self, mocker):
        def populate(transaction):
            transaction.condition_check(MergeModel, 1, 2, condition=MergeModel.mock_toot.exists())
            transaction.update(MergeModel(1, 2), actions=[MergeModel.mock_toot.set('a')])
            transaction.delete(MergeModel(1, 2))

        with pytest.raises(TransactWriteMergeError):
            self._write(mocker, populate)

    def test_identical_puts_merged(self, mocker):
        def populate(transaction):
            transaction.save(MergeModel(1, 2, mock_toot='same'))
            transaction.save(MergeModel(1, 2, mock_toot='same'))

        kwargs = self._write(mocker, populate)
        assert len(kwargs['put_items']) == 1

    def test_non_equivalent_puts_rejected(self, mocker):
        def populate(transaction):
            transaction.save(MergeModel(1, 2, mock_toot='one'))
            transaction.save(MergeModel(1, 2, mock_toot='two'))

        with pytest.raises(TransactWriteMergeError):
            self._write(mocker, populate)

    def test_conflicting_version_conditions_rejected(self, mocker):
        def populate(transaction):
            transaction.update(MergeModel(1, 2, mock_version=1),
                               actions=[MergeModel.mock_toot.set('a')])
            transaction.update(MergeModel(1, 2, mock_version=2),
                               actions=[MergeModel.mock_other.set('b')])

        with pytest.raises(TransactWriteMergeError) as exc_info:
            self._write(mocker, populate)
        assert 'version' in exc_info.value.msg

    def test_return_values_mismatch_rejected(self, mocker):
        def populate(transaction):
            transaction.update(MergeModel(1, 2), actions=[MergeModel.mock_toot.set('a')],
                               return_values='ALL_OLD')
            transaction.update(MergeModel(1, 2), actions=[MergeModel.mock_other.set('b')],
                               return_values='NONE')

        with pytest.raises(TransactWriteMergeError):
            self._write(mocker, populate)

    def test_local_version_updated_once_per_model_after_success(self, mocker):
        connection = Connection()
        mocker.patch.object(connection, 'transact_write_items')
        first = MergeModel(1, 2)
        second = MergeModel(1, 2)
        with TransactWrite(connection=connection, merge=True) as transaction:
            transaction.update(first, actions=[MergeModel.mock_toot.set('a')])
            transaction.update(second, actions=[MergeModel.mock_other.set('b')])
        assert first.mock_version == 1
        assert second.mock_version == 1

    def test_local_version_not_updated_on_failure(self, mocker):
        connection = Connection()
        mocker.patch.object(
            connection, 'transact_write_items',
            side_effect=RuntimeError('dynamodb unavailable'),
        )
        model = MergeModel(1, 2, mock_version=5)
        with pytest.raises(RuntimeError):
            with TransactWrite(connection=connection, merge=True) as transaction:
                transaction.update(model, actions=[MergeModel.mock_toot.set('a')])
        assert model.mock_version == 5

    def test_each_item_appears_once_in_final_request(self, mocker):
        def populate(transaction):
            transaction.condition_check(MergeModel, 1, 2, condition=MergeModel.mock_toot.exists())
            transaction.update(MergeModel(1, 2), actions=[MergeModel.mock_toot.set('a')])
            transaction.update(MergeModel(1, 3), actions=[MergeModel.mock_toot.set('b')])
            transaction.delete(MergeModel(4, 5))

        kwargs = self._write(mocker, populate)
        seen = set()
        for items in (kwargs['condition_check_items'], kwargs['delete_items'],
                      kwargs['put_items'], kwargs['update_items']):
            for item in items:
                identity = item.get('Key') or item['Item']
                marker = (
                    item['TableName'],
                    tuple((name, tuple(sorted(value.items())))
                          for name, value in sorted(identity.items())),
                )
                assert marker not in seen
                seen.add(marker)
        assert len(seen) == 3
