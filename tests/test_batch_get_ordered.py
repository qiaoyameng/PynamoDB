"""
Tests for Model.batch_get_ordered
"""
import json
from unittest import TestCase
from unittest.mock import MagicMock, patch

from pynamodb.attributes import NumberAttribute, UnicodeAttribute
from pynamodb.constants import KEYS, RESPONSES, UNPROCESSED_KEYS
from pynamodb.models import (
    BATCH_GET_NOT_FOUND,
    BATCH_GET_UNKNOWN,
    BatchGetPlaceholder,
    Model,
)

PATCH_METHOD = 'pynamodb.connection.Connection._make_api_call'


class OrderedHashModel(Model):
    """
    A hash key only model
    """

    class Meta:
        table_name = 'OrderedHashModel'

    user_name = UnicodeAttribute(hash_key=True)
    email = UnicodeAttribute(null=True)


class OrderedCompositeModel(Model):
    """
    A model with a hash key and a range key
    """

    class Meta:
        table_name = 'OrderedCompositeModel'

    forum = UnicodeAttribute(hash_key=True)
    subject = UnicodeAttribute(range_key=True)
    views = NumberAttribute(null=True)


class FoldedKeyAttribute(UnicodeAttribute):
    """
    A custom key attribute whose serialization folds case, so distinct
    python values can serialize to the same DynamoDB key.
    """

    def serialize(self, value):
        return super().serialize(value.lower())


class FoldedKeyModel(Model):
    """
    A model whose hash key uses custom serialization
    """

    class Meta:
        table_name = 'FoldedKeyModel'

    user_name = FoldedKeyAttribute(hash_key=True)
    email = UnicodeAttribute(null=True)


def _wire_key(key_dict):
    return json.dumps(key_dict, sort_keys=True)


def hash_item(name, email=None):
    item = {'user_name': {'S': name}}
    if email is not None:
        item['email'] = {'S': email}
    return item


def composite_item(forum, subject, views=None):
    item = {'forum': {'S': forum}, 'subject': {'S': subject}}
    if views is not None:
        item['views'] = {'N': str(views)}
    return item


class BatchGetStub:
    """
    A configurable stub for `Connection._make_api_call` that serves BatchGetItem
    requests out of order and defers selected keys to UnprocessedKeys for a
    fixed number of rounds. Keys absent from the table are silently dropped
    from the response, mimicking DynamoDB's behavior for nonexistent items.
    """

    def __init__(self, table_name, items, key_names=('user_name',), defer_rounds=None):
        self.table_name = table_name
        self.items = {
            _wire_key({name: item[name] for name in key_names}): item
            for item in items
        }
        self.defer_rounds = dict(defer_rounds or {})
        self.requests = []

    def __call__(self, operation_name, operation_kwargs):
        assert operation_name == 'BatchGetItem'
        keys = operation_kwargs['RequestItems'][self.table_name]['Keys']
        self.requests.append(keys)
        responses = []
        unprocessed = []
        for key in keys:
            item = self.items.get(_wire_key(key))
            if item is None:
                continue
            defer = self.defer_rounds.get(_wire_key(key), 0)
            if defer > 0:
                self.defer_rounds[_wire_key(key)] = defer - 1
                unprocessed.append(key)
            else:
                responses.append(item)
        responses.reverse()
        return {
            RESPONSES: {self.table_name: responses},
            UNPROCESSED_KEYS: {self.table_name: {KEYS: unprocessed}} if unprocessed else {},
        }

    @property
    def requested_keys(self):
        return [key for call in self.requests for key in call]


class BatchGetOrderedTest(TestCase):

    def test_preserves_order_and_marks_missing(self):
        stub = BatchGetStub('OrderedHashModel', [hash_item('a'), hash_item('b')])
        with patch(PATCH_METHOD, new=stub):
            results = OrderedHashModel.batch_get_ordered(['b', 'missing', 'a', 'b'])

        assert len(results) == 4
        assert isinstance(results[0], OrderedHashModel)
        assert results[0].user_name == 'b'
        assert results[1] is BATCH_GET_NOT_FOUND
        assert isinstance(results[2], OrderedHashModel)
        assert results[2].user_name == 'a'
        assert isinstance(results[3], OrderedHashModel)
        assert results[3].user_name == 'b'

    def test_duplicate_keys_requested_once(self):
        stub = BatchGetStub('OrderedHashModel', [hash_item('a'), hash_item('b')])
        with patch(PATCH_METHOD, new=stub):
            OrderedHashModel.batch_get_ordered(['b', 'a', 'b', 'b', 'a'])

        assert len(stub.requests) == 1
        assert sorted(stub.requested_keys, key=_wire_key) == [
            {'user_name': {'S': 'a'}},
            {'user_name': {'S': 'b'}},
        ]

    def test_duplicate_positions_do_not_share_instances(self):
        stub = BatchGetStub('OrderedHashModel', [hash_item('a', email='a@example.com')])
        with patch(PATCH_METHOD, new=stub):
            results = OrderedHashModel.batch_get_ordered(['a', 'a'])

        assert results[0] is not results[1]
        assert results[0].user_name == results[1].user_name == 'a'
        results[0].email = 'changed@example.com'
        assert results[1].email == 'a@example.com'

    def test_unprocessed_keys_retried_until_processed(self):
        stub = BatchGetStub(
            'OrderedHashModel',
            [hash_item('a'), hash_item('b')],
            defer_rounds={_wire_key({'user_name': {'S': 'b'}}): 2},
        )
        with patch(PATCH_METHOD, new=stub):
            results = OrderedHashModel.batch_get_ordered(['a', 'b'])

        assert len(stub.requests) == 3
        assert results[0].user_name == 'a'
        assert results[1].user_name == 'b'
        # only the unprocessed key is retried
        assert stub.requests[1] == [{'user_name': {'S': 'b'}}]
        assert stub.requests[2] == [{'user_name': {'S': 'b'}}]

    def test_unprocessed_keys_exhausted_becomes_unknown(self):
        stub = BatchGetStub(
            'OrderedHashModel',
            [hash_item('a'), hash_item('b')],
            defer_rounds={_wire_key({'user_name': {'S': 'b'}}): 10},
        )
        with patch(PATCH_METHOD, new=stub):
            results = OrderedHashModel.batch_get_ordered(['a', 'b', 'b'], max_retries=2)

        assert len(stub.requests) == 3  # initial attempt + 2 retries
        assert results[0].user_name == 'a'
        assert results[1] is BATCH_GET_UNKNOWN
        assert results[2] is BATCH_GET_UNKNOWN
        assert BATCH_GET_UNKNOWN is not BATCH_GET_NOT_FOUND
        assert isinstance(BATCH_GET_UNKNOWN, BatchGetPlaceholder)

    def test_composite_keys(self):
        items = [
            composite_item('f1', 's1', views=1),
            composite_item('f1', 's2', views=2),
            composite_item('f2', 's1', views=3),
        ]
        stub = BatchGetStub(
            'OrderedCompositeModel',
            items,
            key_names=('forum', 'subject'),
            defer_rounds={_wire_key({'forum': {'S': 'f1'}, 'subject': {'S': 's2'}}): 1},
        )
        with patch(PATCH_METHOD, new=stub):
            results = OrderedCompositeModel.batch_get_ordered([
                ('f2', 's1'),
                ('f1', 's2'),
                ('f1', 's1'),
                ('f1', 's2'),
                ('f1', 'missing'),
            ])

        assert [r.subject if isinstance(r, OrderedCompositeModel) else r for r in results] == [
            's1', 's2', 's1', 's2', BATCH_GET_NOT_FOUND,
        ]
        assert results[0].forum == 'f2'
        assert results[1] is not results[3]
        results[1].views = 99
        assert results[3].views == 2
        # the same hash key with different range keys (and vice versa) are distinct requests
        assert len(stub.requests[0]) == 4

    def test_custom_serialization_key_equivalence(self):
        stub = BatchGetStub('FoldedKeyModel', [hash_item('alice', email='a@example.com')])
        with patch(PATCH_METHOD, new=stub):
            results = FoldedKeyModel.batch_get_ordered(['Alice', 'ALICE', 'bob'])

        # 'Alice' and 'ALICE' serialize to the same key and are requested once
        assert stub.requested_keys == [
            {'user_name': {'S': 'alice'}},
            {'user_name': {'S': 'bob'}},
        ]
        assert results[0].user_name == 'alice'
        assert results[1].user_name == 'alice'
        assert results[0] is not results[1]
        assert results[2] is BATCH_GET_NOT_FOUND

    def test_pagination_preserves_positions(self):
        names = ['user-{}'.format(x) for x in range(150)]
        stub = BatchGetStub('OrderedHashModel', [hash_item(name) for name in names])
        requested = list(reversed(names)) + ['user-0', 'ghost']
        with patch(PATCH_METHOD, new=stub):
            results = OrderedHashModel.batch_get_ordered(requested)

        assert len(stub.requests) == 2
        assert len(stub.requests[0]) == 100
        assert len(stub.requests[1]) == 51  # 50 remaining unique keys + 'ghost'
        for result, name in zip(results, requested):
            if name == 'ghost':
                assert result is BATCH_GET_NOT_FOUND
            else:
                assert result.user_name == name

    def test_empty_input_makes_no_requests(self):
        mock = MagicMock()
        with patch(PATCH_METHOD, new=mock):
            assert OrderedHashModel.batch_get_ordered([]) == []
        mock.assert_not_called()

    def test_invalid_key_raises(self):
        with patch(PATCH_METHOD, new=MagicMock()):
            with self.assertRaises(ValueError):
                OrderedCompositeModel.batch_get_ordered(['just-a-string'])
            with self.assertRaises(ValueError):
                OrderedCompositeModel.batch_get_ordered([('a', 'b', 'c')])
