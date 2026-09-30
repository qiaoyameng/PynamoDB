import pytest
from pynamodb.attributes import NumberAttribute, UnicodeAttribute, VersionAttribute

from pynamodb.connection import Connection
from pynamodb.connection.base import MetaTable
from pynamodb.constants import ALL_OLD, TABLE_KEY
from pynamodb.exceptions import CancellationReason, TransactWriteError, VerboseClientError
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


class FirstModel(MockModel):
    pass


class SecondModel(MockModel):
    pass


class ThirdModel(MockModel):
    pass


class FourthModel(MockModel):
    pass


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

    def test_commit__condition_failure__decodes_reasons_for_models(self, mocker):
        connection = Connection()
        mock_connection_transact_write = mocker.patch.object(connection, 'transact_write_items')

        raw_first_item = {
            'mock_hash': {'N': '1'},
            'mock_range': {'N': '1'},
            'mock_toot': {'S': 'first'},
            'mock_version': {'N': '7'},
        }
        raw_third_item = {
            'mock_hash': {'N': '3'},
            'mock_range': {'N': '3'},
            'mock_toot': {'S': 'third'},
            'mock_version': {'N': '9'},
        }
        raw_fourth_item = {
            'mock_hash': {'N': '4'},
            'mock_range': {'N': '4'},
            'mock_toot': {'S': 'fourth'},
            'mock_version': {'N': '10'},
        }
        cause = VerboseClientError(
            {'Error': {'Code': 'TransactionCanceledException', 'Message': 'cancelled'}},
            'TransactWriteItems',
            cancellation_reasons=[
                CancellationReason(code='ConditionalCheckFailed', message='failed', raw_item=raw_first_item),
                None,
                CancellationReason(code='ConditionalCheckFailed', message='failed', raw_item=raw_third_item),
                CancellationReason(code='ConditionalCheckFailed', message='failed', raw_item=raw_fourth_item),
            ],
        )
        mock_connection_transact_write.side_effect = TransactWriteError('cancelled', cause)

        first_model = FirstModel(1, 1)
        second_model = SecondModel(2, 2, mock_version=5)
        third_model = ThirdModel(3, 3, mock_version=5)
        fourth_model = FourthModel(4, 4, mock_version=5)

        with pytest.raises(TransactWriteError) as exc_info:
            with TransactWrite(connection=connection) as transaction:
                transaction.condition_check(
                    FirstModel,
                    1,
                    1,
                    condition=FirstModel.mock_hash.exists(),
                    return_values=ALL_OLD,
                )
                transaction.delete(second_model, return_values=ALL_OLD)
                transaction.save(third_model, return_values=ALL_OLD)
                transaction.update(
                    fourth_model,
                    actions=[FourthModel.mock_toot.set('updated')],
                    return_values=ALL_OLD,
                )

        reasons = exc_info.value.cancellation_reasons
        assert reasons[0].code == 'ConditionalCheckFailed'
        assert reasons[1] is None
        assert reasons[2].code == 'ConditionalCheckFailed'
        assert reasons[3].code == 'ConditionalCheckFailed'
        assert isinstance(reasons[0].item, FirstModel)
        assert isinstance(reasons[2].item, ThirdModel)
        assert isinstance(reasons[3].item, FourthModel)
        assert reasons[2].item is not third_model
        assert reasons[3].item is not fourth_model
        assert reasons[0].item.mock_toot == 'first'
        assert reasons[2].item.mock_version == 9
        assert reasons[3].item.mock_version == 10
        assert reasons[0].raw_item == raw_first_item
        assert second_model.mock_version == 5
        assert third_model.mock_version == 5
        assert fourth_model.mock_version == 5
        assert mock_connection_transact_write.call_args.kwargs['condition_check_items'][0] == {
            'Key': {'mock_hash': {'N': '1'}, 'mock_range': {'N': '1'}},
            'TableName': 'mock',
            'ConditionExpression': 'attribute_exists (#0)',
            'ExpressionAttributeNames': {'#0': 'mock_hash'},
            'ReturnValuesOnConditionCheckFailure': 'ALL_OLD',
        }

    def test_commit__condition_failure__missing_and_invalid_items(self, mocker):
        connection = Connection()
        mock_connection_transact_write = mocker.patch.object(connection, 'transact_write_items')
        invalid_item = {'mock_hash': {'N': '1'}, 'mock_version': {'S': 'not-a-number'}}
        cause = VerboseClientError(
            {'Error': {'Code': 'TransactionCanceledException', 'Message': 'cancelled'}},
            'TransactWriteItems',
            cancellation_reasons=[
                CancellationReason(code='ConditionalCheckFailed', message='failed', raw_item=invalid_item),
                CancellationReason(code='ConditionalCheckFailed', message='failed'),
            ],
        )
        mock_connection_transact_write.side_effect = TransactWriteError('cancelled', cause)

        with pytest.raises(TransactWriteError) as exc_info:
            with TransactWrite(connection=connection) as transaction:
                transaction.condition_check(
                    MockModel,
                    1,
                    condition=MockModel.mock_hash.exists(),
                    return_values=ALL_OLD,
                )
                transaction.save(MockModel(2, 2), return_values=ALL_OLD)

        reasons = exc_info.value.cancellation_reasons
        assert reasons[0].code == 'ConditionalCheckFailed'
        assert reasons[0].raw_item == invalid_item
        assert reasons[0].item is None
        assert reasons[1].code == 'ConditionalCheckFailed'
        assert reasons[1].raw_item is None
        assert reasons[1].item is None
