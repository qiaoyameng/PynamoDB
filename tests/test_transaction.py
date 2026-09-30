import pytest
from pynamodb.attributes import NumberAttribute, UnicodeAttribute, VersionAttribute

from pynamodb.connection import Connection
from pynamodb.connection.base import MetaTable
from pynamodb.constants import TABLE_KEY
from pynamodb.exceptions import CancellationReason, TransactWriteError
from pynamodb.exceptions import VerboseClientError
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


class OtherMockModel(Model):
    class Meta:
        table_name = 'other-mock'

    other_hash = NumberAttribute(hash_key=True)
    name = UnicodeAttribute(null=True)
    version = VersionAttribute()


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
            t.delete(MockModel(2, 4), return_values='ALL_OLD')
            t.save(MockModel(3, 5))
            t.update(MockModel(4, 6), actions=[MockModel.mock_toot.set('hello')], return_values='ALL_OLD')

        expected_condition_check = {
            'ConditionExpression': 'attribute_not_exists (#0)',
            'ExpressionAttributeNames': {'#0': 'mock_hash'},
            'Key': {'mock_hash': {'N': '1'}, 'mock_range': {'N': '3'}},
            'TableName': 'mock'
        }
        expected_delete = {
            'ConditionExpression': 'attribute_not_exists (#0)',
            'ExpressionAttributeNames': {'#0': 'mock_version'},
            'Key': {'mock_hash': {'N': '2'}, 'mock_range': {'N': '4'}},
            'TableName': 'mock',
            'ReturnValuesOnConditionCheckFailure': 'ALL_OLD',
        }
        expected_put = {
            'ConditionExpression': 'attribute_not_exists (#0)',
            'ExpressionAttributeNames': {'#0': 'mock_version'},
            'Item': {'mock_hash': {'N': '3'}, 'mock_range': {'N': '5'}, 'mock_version': {'N': '1'}},
            'TableName': 'mock'
        }
        expected_update = {
            'ConditionExpression': 'attribute_not_exists (#0)',
            'TableName': 'mock',
            'Key': {'mock_hash': {'N': '4'}, 'mock_range': {'N': '6'}},
            'ReturnValuesOnConditionCheckFailure': 'ALL_OLD',
            'UpdateExpression': 'SET #1 = :0, #0 = :1',
            'ExpressionAttributeNames': {'#0': 'mock_version', '#1': 'mock_toot'},
            'ExpressionAttributeValues': {':0': {'S': 'hello'}, ':1': {'N': '1'}}
        }
        mock_connection_transact_write.assert_called_once_with(
            condition_check_items=[],
            delete_items=[],
            put_items=[],
            update_items=[],
            client_request_token=None,
            return_consumed_capacity=None,
            return_item_collection_metrics=None,
            transact_items=[
                {'ConditionCheck': expected_condition_check},
                {'Delete': expected_delete},
                {'Put': expected_put},
                {'Update': expected_update},
            ],
        )

    def test_commit__cancellation_reasons_decode_old_items_in_action_order(
        self,
        mocker,
    ):
        connection = Connection()
        mock_connection_transact_write = mocker.patch.object(
            connection,
            'transact_write_items',
        )
        mock_connection_transact_write.side_effect = TransactWriteError(
            'failure',
            VerboseClientError(
                {
                    'Error': {
                        'Code': 'TransactionCanceledException',
                        'Message': 'Transaction cancelled',
                    }
                },
                'TransactWriteItems',
                cancellation_reasons=[
                    CancellationReason(
                        code='ConditionalCheckFailed',
                        message='The conditional request failed',
                        raw_item={
                            'other_hash': {'N': '10'},
                            'name': {'S': 'old name'},
                            'version': {'N': '3'},
                        },
                    ),
                    None,
                    CancellationReason(
                        code='ConditionalCheckFailed',
                        message='The conditional request failed',
                        raw_item={
                            'mock_hash': {'N': '1'},
                            'mock_range': {'N': '2'},
                            'mock_toot': {'S': 'old'},
                            'mock_version': {'N': '7'},
                        },
                    ),
                    CancellationReason(code='ValidationError', message='Bad update'),
                ],
            ),
        )

        put_model = OtherMockModel(20, name='new')
        update_model = MockModel(1, 2, mock_toot='new', mock_version=7)
        with pytest.raises(TransactWriteError) as exc_info:
            with TransactWrite(connection=connection) as transaction:
                transaction.condition_check(
                    OtherMockModel,
                    10,
                    condition=OtherMockModel.name.exists(),
                    return_values='ALL_OLD',
                )
                transaction.save(put_model, return_values='ALL_OLD')
                transaction.update(
                    update_model,
                    actions=[MockModel.mock_toot.set('changed')],
                    return_values='ALL_OLD',
                )
                transaction.delete(MockModel(9, 9), return_values='ALL_OLD')

        reasons = exc_info.value.cancellation_reasons
        assert [reason.code if reason else None for reason in reasons] == [
            'ConditionalCheckFailed',
            None,
            'ConditionalCheckFailed',
            'ValidationError',
        ]
        assert isinstance(reasons[0].item, OtherMockModel)
        assert reasons[0].item.other_hash == 10
        assert reasons[0].item.name == 'old name'
        assert reasons[0].item.version == 3
        assert isinstance(reasons[2].item, MockModel)
        assert reasons[2].item is not update_model
        assert reasons[2].item.mock_hash == 1
        assert reasons[2].item.mock_range == 2
        assert reasons[2].item.mock_toot == 'old'
        assert reasons[2].item.mock_version == 7
        assert reasons[3].item is None
        assert put_model.version is None
        assert update_model.mock_version == 7

    def test_commit__invalid_old_item_does_not_mask_condition_failure(self, mocker):
        connection = Connection()
        mock_connection_transact_write = mocker.patch.object(
            connection,
            'transact_write_items',
        )
        raw_item = {
            'other_hash': {'N': 'not-a-number'},
            'name': {'S': 'old name'},
        }
        mock_connection_transact_write.side_effect = TransactWriteError(
            'failure',
            VerboseClientError(
                {
                    'Error': {
                        'Code': 'TransactionCanceledException',
                        'Message': 'Transaction cancelled',
                    }
                },
                'TransactWriteItems',
                cancellation_reasons=[
                    CancellationReason(code='ConditionalCheckFailed', raw_item=raw_item),
                ],
            ),
        )

        with pytest.raises(TransactWriteError) as exc_info:
            with TransactWrite(connection=connection) as transaction:
                transaction.condition_check(
                    OtherMockModel,
                    10,
                    condition=OtherMockModel.name.exists(),
                    return_values='ALL_OLD',
                )

        reason = exc_info.value.cancellation_reasons[0]
        assert reason.code == 'ConditionalCheckFailed'
        assert reason.raw_item == raw_item
        assert reason.item is None
