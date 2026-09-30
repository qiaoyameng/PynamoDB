import pytest

from pynamodb.attributes import NumberAttribute, NumberSetAttribute, UnicodeAttribute
from pynamodb.connection import Connection
from pynamodb.connection.base import MetaTable
from pynamodb.constants import TABLE_KEY
from pynamodb.exceptions import TransactWriteMergeError
from pynamodb.models import Model
from pynamodb.transactions import TransactWrite

from tests.test_transaction import MOCK_TABLE_DESCRIPTOR, MockModel


@pytest.fixture
def connection():
    connection = Connection()
    connection.add_meta_table(MetaTable(MOCK_TABLE_DESCRIPTOR[TABLE_KEY]))
    return connection


def _write(connection, *actions):
    transaction = TransactWrite(connection=connection, merge_actions=True)
    for action in actions:
        action(transaction)
    transaction._commit()
    return transaction._connection.transact_write_items.call_args.kwargs


class TestTransactWriteMerge:

    def test_disabled_by_default_keeps_duplicate_actions(self, connection, mocker):
        write_mock = mocker.patch.object(connection, 'transact_write_items')
        with TransactWrite(connection=connection) as transaction:
            transaction.update(MockModel(1, 2), actions=[MockModel.mock_toot.set('a')])
            transaction.update(MockModel(1, 2), actions=[MockModel.mock_toot.set('b')])
        assert len(write_mock.call_args.kwargs['update_items']) == 2

    def test_merges_non_conflicting_updates_into_one(self, connection, mocker):
        mocker.patch.object(connection, 'transact_write_items')
        kwargs = _write(
            connection,
            lambda t: t.update(MockModel(1, 2), actions=[MockModel.mock_toot.set('a')]),
            lambda t: t.update(
                MockModel(1, 2),
                actions=[MockModel.mock_range.add(1)],
                condition=MockModel.mock_hash.exists(),
            ),
        )
        assert len(kwargs['update_items']) == 1
        update = kwargs['update_items'][0]
        assert update['Key'] == {'mock_hash': {'N': '1'}, 'mock_range': {'N': '2'}}
        assert 'SET' in update['UpdateExpression']
        assert 'ADD' in update['UpdateExpression']
        assert 'AND' in update['ConditionExpression']

    def test_each_item_appears_once_with_condition_check_folded(self, connection, mocker):
        mocker.patch.object(connection, 'transact_write_items')
        kwargs = _write(
            connection,
            lambda t: t.update(MockModel(1, 2), actions=[MockModel.mock_toot.set('a')]),
            lambda t: t.condition_check(MockModel, 1, 2, condition=MockModel.mock_hash.exists()),
            lambda t: t.update(MockModel(1, 2), actions=[MockModel.mock_range.add(1)]),
        )
        total = sum(
            len(kwargs[key])
            for key in ('condition_check_items', 'delete_items', 'put_items', 'update_items')
        )
        assert total == 1

    def test_different_items_are_not_merged(self, connection, mocker):
        mocker.patch.object(connection, 'transact_write_items')
        kwargs = _write(
            connection,
            lambda t: t.update(MockModel(1, 2), actions=[MockModel.mock_toot.set('a')]),
            lambda t: t.update(MockModel(1, 3), actions=[MockModel.mock_toot.set('b')]),
        )
        assert len(kwargs['update_items']) == 2

    def test_numeric_key_forms_are_normalized(self, connection, mocker):
        mocker.patch.object(connection, 'transact_write_items')
        kwargs = _write(
            connection,
            lambda t: t.update(MockModel(1, 2), actions=[MockModel.mock_toot.set('a')]),
            lambda t: t.update(MockModel(1.0, 2.0), actions=[MockModel.mock_range.add(1)]),
        )
        assert len(kwargs['update_items']) == 1

    def test_rejects_delete_and_update_of_same_item(self, connection, mocker):
        write_mock = mocker.patch.object(connection, 'transact_write_items')
        with pytest.raises(TransactWriteMergeError) as exc_info:
            _write(
                connection,
                lambda t: t.delete(MockModel(1, 2)),
                lambda t: t.update(MockModel(1, 2), actions=[MockModel.mock_toot.set('a')]),
            )
        message = str(exc_info.value)
        assert 'delete' in message and 'update' in message
        assert '#0' in message and '#1' in message
        write_mock.assert_not_called()

    def test_rejects_parent_child_document_paths(self, connection, mocker):
        mocker.patch.object(connection, 'transact_write_items')
        with pytest.raises(TransactWriteMergeError) as exc_info:
            _write(
                connection,
                lambda t: t.update(MockModel(1, 2), actions=[MockModel.mock_toot.set('a')]),
                lambda t: t.update(MockModel(1, 2), actions=[MockModel.mock_toot.remove()]),
            )
        assert 'document paths' in str(exc_info.value)

    def test_sibling_paths_are_allowed(self, connection, mocker):
        mocker.patch.object(connection, 'transact_write_items')
        kwargs = _write(
            connection,
            lambda t: t.update(
                MockModel(1, 2),
                actions=[MockModel.mock_hash.add(1), MockModel.mock_range.add(1)],
                add_version_condition=False,
            ),
        )
        assert kwargs['update_items'][0]['UpdateExpression'].count('ADD') == 1

    def test_rejects_duplicate_non_idempotent_number_add(self, connection, mocker):
        mocker.patch.object(connection, 'transact_write_items')
        with pytest.raises(TransactWriteMergeError) as exc_info:
            _write(
                connection,
                lambda t: t.update(
                    MockModel(1, 2), actions=[MockModel.mock_range.add(1)], add_version_condition=False
                ),
                lambda t: t.update(
                    MockModel(1, 2), actions=[MockModel.mock_range.add(1)], add_version_condition=False
                ),
            )
        assert 'duplicate' in str(exc_info.value)

    def test_duplicate_set_action_is_collapsed(self, connection, mocker):
        mocker.patch.object(connection, 'transact_write_items')
        kwargs = _write(
            connection,
            lambda t: t.update(
                MockModel(1, 2), actions=[MockModel.mock_toot.set('a')], add_version_condition=False
            ),
            lambda t: t.update(
                MockModel(1, 2), actions=[MockModel.mock_toot.set('a')], add_version_condition=False
            ),
        )
        # the duplicate SET collapses; only one 'a' value placeholder remains
        update = kwargs['update_items'][0]
        assert update['UpdateExpression'].count('#0 = :0') == 1
        assert list(update['ExpressionAttributeValues'].values()).count({'S': 'a'}) == 1
        # the version condition is not added when add_version_condition=False
        assert 'ConditionExpression' not in update

    def test_rejects_different_set_values_on_same_path(self, connection, mocker):
        mocker.patch.object(connection, 'transact_write_items')
        with pytest.raises(TransactWriteMergeError):
            _write(
                connection,
                lambda t: t.update(
                    MockModel(1, 2), actions=[MockModel.mock_toot.set('a')], add_version_condition=False
                ),
                lambda t: t.update(
                    MockModel(1, 2), actions=[MockModel.mock_toot.set('b')], add_version_condition=False
                ),
            )

    def test_rejects_conflicting_puts(self, connection, mocker):
        mocker.patch.object(connection, 'transact_write_items')
        first = MockModel(1, 2)
        first.mock_toot = 'a'
        second = MockModel(1, 2)
        second.mock_toot = 'b'
        with pytest.raises(TransactWriteMergeError) as exc_info:
            _write(
                connection,
                lambda t: t.save(first),
                lambda t: t.save(second),
            )
        assert 'different attribute values' in str(exc_info.value)

    def test_rejects_put_with_update(self, connection, mocker):
        mocker.patch.object(connection, 'transact_write_items')
        with pytest.raises(TransactWriteMergeError):
            _write(
                connection,
                lambda t: t.save(MockModel(1, 2)),
                lambda t: t.update(MockModel(1, 2), actions=[MockModel.mock_toot.set('a')]),
            )

    def test_version_conditions_are_anded_once(self, connection, mocker):
        mocker.patch.object(connection, 'transact_write_items')
        first = MockModel(1, 2)
        first.mock_version = 3
        second = MockModel(1, 2)
        second.mock_version = 3
        kwargs = _write(
            connection,
            lambda t: t.update(first, actions=[MockModel.mock_toot.set('a')]),
            lambda t: t.update(second, actions=[MockModel.mock_range.add(1)]),
        )
        update = kwargs['update_items'][0]
        version_placeholders = [
            placeholder for placeholder, name in update['ExpressionAttributeNames'].items()
            if name == 'mock_version'
        ]
        assert len(version_placeholders) == 1
        assert [value for value in update['ExpressionAttributeValues'].values() if value == {'N': '3'}]
        assert update['UpdateExpression'].count('ADD') == 1

    def test_rejects_diverging_version_expectations(self, connection, mocker):
        write_mock = mocker.patch.object(connection, 'transact_write_items')
        first = MockModel(1, 2)
        first.mock_version = 1
        second = MockModel(1, 2)
        second.mock_version = 2
        with pytest.raises(TransactWriteMergeError) as exc_info:
            _write(
                connection,
                lambda t: t.update(first, actions=[MockModel.mock_toot.set('a')]),
                lambda t: t.update(second, actions=[MockModel.mock_range.add(1)]),
            )
        assert 'version' in str(exc_info.value)
        write_mock.assert_not_called()

    def test_local_version_updated_only_after_success(self, connection, mocker):
        mocker.patch.object(connection, 'transact_write_items')
        model = MockModel(1, 2)
        model.mock_version = 5
        with TransactWrite(connection=connection, merge_actions=True) as transaction:
            transaction.update(model, actions=[MockModel.mock_toot.set('a')])
        assert model.mock_version == 6

    def test_local_version_not_updated_on_failure(self, connection, mocker):
        write_mock = mocker.patch.object(connection, 'transact_write_items')
        write_mock.side_effect = RuntimeError('dynamodb unavailable')
        model = MockModel(1, 2)
        model.mock_version = 5
        with pytest.raises(RuntimeError):
            with TransactWrite(connection=connection, merge_actions=True) as transaction:
                transaction.update(model, actions=[MockModel.mock_toot.set('a')])
        assert model.mock_version == 5

    def test_conflict_error_lists_original_positions_and_caller(self, connection, mocker):
        write_mock = mocker.patch.object(connection, 'transact_write_items')
        with pytest.raises(TransactWriteMergeError) as exc_info:
            with TransactWrite(connection=connection, merge_actions=True) as transaction:
                transaction.update(
                    MockModel(1, 2), actions=[MockModel.mock_toot.set('a')], add_version_condition=False
                )
                transaction.update(
                    MockModel(1, 2), actions=[MockModel.mock_toot.set('b')], add_version_condition=False
                )
        message = str(exc_info.value)
        assert '#0' in message and '#1' in message
        assert 'test_transaction_merge.py' in message
        write_mock.assert_not_called()


class TestMergeSetAndAliasActions:

    def test_duplicate_set_add_is_idempotent(self, connection, mocker):
        mocker.patch.object(connection, 'transact_write_items')

        class SetModel(Model):
            class Meta:
                table_name = 'mock'
            hk = NumberAttribute(hash_key=True, attr_name='mock_hash')
            tags = NumberSetAttribute(null=True, attr_name='mock_toot')

        kwargs = _write(
            connection,
            lambda t: t.update(
                SetModel(1), actions=[SetModel.tags.add({1, 2})], add_version_condition=False
            ),
            lambda t: t.update(
                SetModel(1), actions=[SetModel.tags.add({1, 2})], add_version_condition=False
            ),
        )
        assert kwargs['update_items'][0]['UpdateExpression'] == 'ADD #0 :0'

    def test_alias_attributes_referring_same_name_conflict(self, connection, mocker):
        mocker.patch.object(connection, 'transact_write_items')

        class AliasModel(Model):
            class Meta:
                table_name = 'mock'
            hk = NumberAttribute(hash_key=True, attr_name='mock_hash')
            rk = NumberAttribute(range_key=True, attr_name='mock_range')
            first = UnicodeAttribute(null=True, attr_name='shared')
            second = UnicodeAttribute(null=True, attr_name='shared')

        with pytest.raises(TransactWriteMergeError):
            _write(
                connection,
                lambda t: t.update(
                    AliasModel(1, 2), actions=[AliasModel.first.set('a')], add_version_condition=False
                ),
                lambda t: t.update(
                    AliasModel(1, 2), actions=[AliasModel.second.set('b')], add_version_condition=False
                ),
            )


class TestMergeDeleteAndConditionCheck:

    def test_duplicate_deletes_merge_with_and_conditions(self, connection, mocker):
        mocker.patch.object(connection, 'transact_write_items')
        first = MockModel(1, 2)
        first.mock_version = 7
        second = MockModel(1, 2)
        second.mock_version = 7
        kwargs = _write(
            connection,
            lambda t: t.delete(first, condition=MockModel.mock_hash.exists()),
            lambda t: t.condition_check(MockModel, 1, 2, condition=MockModel.mock_range.exists()),
            lambda t: t.delete(second),
        )
        assert len(kwargs['delete_items']) == 1
        delete = kwargs['delete_items'][0]
        assert delete['Key'] == {'mock_hash': {'N': '1'}, 'mock_range': {'N': '2'}}
        assert delete['ConditionExpression'].count('AND') == 2
        assert delete['ExpressionAttributeValues'] == {':0': {'N': '7'}}

    def test_condition_checks_are_not_double_serialized(self, connection, mocker):
        mocker.patch.object(connection, 'transact_write_items')
        kwargs = _write(
            connection,
            lambda t: t.condition_check(MockModel, 1, 2, condition=MockModel.mock_hash.exists()),
            lambda t: t.condition_check(MockModel, 1, 2, condition=MockModel.mock_range.exists()),
        )
        assert len(kwargs['condition_check_items']) == 1
        check = kwargs['condition_check_items'][0]
        assert check['Key'] == {'mock_hash': {'N': '1'}, 'mock_range': {'N': '2'}}

    def test_rejects_deletes_with_diverging_versions(self, connection, mocker):
        write_mock = mocker.patch.object(connection, 'transact_write_items')
        first = MockModel(2, 2)
        first.mock_version = 1
        second = MockModel(2, 2)
        second.mock_version = 2
        with pytest.raises(TransactWriteMergeError) as exc_info:
            _write(
                connection,
                lambda t: t.delete(first),
                lambda t: t.delete(second),
            )
        assert 'version' in str(exc_info.value)
        write_mock.assert_not_called()
