import base64
import re
import sys
from collections import OrderedDict
from decimal import Decimal
from typing import Tuple, TypeVar, Type, Any, List, Optional, Dict, Sequence, NamedTuple

from pynamodb.connection import Connection
from pynamodb.constants import (
    BINARY, BINARY_SET, ITEM, KEY, NUMBER, NUMBER_SET, RESPONSES, STRING_SET, TABLE_NAME,
)
from pynamodb.exceptions import TransactWriteMergeError
from pynamodb.expressions.condition import Condition
from pynamodb.expressions.operand import Path, Value, _Operand
from pynamodb.expressions.update import (
    Action, AddAction, DeleteAction, RemoveAction, SetAction, Update,
)
from pynamodb.models import Model, _ModelFuture, _KeyType

_M = TypeVar('_M', bound=Model)
_TTransaction = TypeVar('_TTransaction', bound='Transaction')

_CONDITION_CHECK = 'condition_check'
_DELETE = 'delete'
_PUT = 'put'
_UPDATE = 'update'


class _CallLocation(NamedTuple):
    filename: str
    lineno: int
    function: str

    def __str__(self) -> str:
        return '{}:{} in {}'.format(self.filename, self.lineno, self.function)


def _capture_call_location() -> Optional[_CallLocation]:
    """
    Captures the location of the user code that enqueued an action, i.e. the
    first frame outside of this module.
    """
    frame = sys._getframe(1)
    this_filename = frame.f_code.co_filename
    while frame is not None and frame.f_code.co_filename == this_filename:
        frame = frame.f_back
    if frame is None:
        return None
    return _CallLocation(frame.f_code.co_filename, frame.f_lineno, frame.f_code.co_name)


def _canonical_dynamodb_value(value: Dict[str, Any]) -> Any:
    """
    Returns a hashable, normalized representation of a serialized DynamoDB
    attribute value. Number attributes are normalized numerically (so that, for
    example, ``1`` and ``1.0`` are treated as the same key), and binary values
    are decoded so that equal bytes compare equal.
    """
    if not isinstance(value, dict) or len(value) != 1:
        return repr(value)
    (attr_type, attr_value), = value.items()
    if attr_type == NUMBER:
        return NUMBER, _canonical_number(attr_value)
    if attr_type == BINARY:
        return BINARY, base64.b64decode(attr_value)
    if attr_type in (NUMBER_SET, STRING_SET, BINARY_SET):
        if attr_type == NUMBER_SET:
            elements = tuple(sorted(_canonical_number(element) for element in attr_value))
        elif attr_type == BINARY_SET:
            elements = tuple(sorted(base64.b64decode(element) for element in attr_value))
        else:
            elements = tuple(sorted(attr_value))
        return attr_type, elements
    return attr_type, attr_value


def _canonical_number(value: str) -> str:
    # Normalizes the wire representation so numerically equal values (e.g.
    # '1' and '1.0') compare as the same key.
    number = Decimal(value)
    return format(number.normalize(), 'f') if number != 0 else '0'


def _canonical_key(key: Dict[str, Dict[str, Any]]) -> Tuple:
    """
    Normalizes a serialized item key (a mapping of key attribute name to a
    typed value) so the same logical item always produces the same identity.
    """
    return tuple(sorted(
        (attr_name, _canonical_dynamodb_value(attr_value))
        for attr_name, attr_value in key.items()
    ))


def _path_segments(path: Path) -> Tuple[str, ...]:
    return tuple(path.path)


_LIST_INDEX_REGEX = re.compile(r'([^\[\]]+)((?:\[\d+\])*)$')


def _split_path_segment(segment: str) -> Tuple[str, Tuple[int, ...]]:
    match = _LIST_INDEX_REGEX.match(segment)
    if match is None:
        raise ValueError('{} is not a valid document path'.format(segment))
    name, indexes = match.groups()
    return name, tuple(int(index) for index in re.findall(r'\d+', indexes))


def _flatten_path(path: Sequence[str]) -> List[Tuple[Any, ...]]:
    steps: List[Tuple[Any, ...]] = []
    for segment in path:
        name, indexes = _split_path_segment(segment)
        steps.append(('name', name))
        steps.extend(('index', index) for index in indexes)
    return steps


def _paths_overlap(path1: Sequence[str], path2: Sequence[str]) -> bool:
    """
    Returns whether two document paths conflict because one is an ancestor of
    the other (parent/child) or they are identical. Sibling paths (e.g.
    ``['a']`` and ``['b']``, or ``['a[0]']`` and ``['a[1]']``) do not conflict.
    """
    steps1 = _flatten_path(path1)
    steps2 = _flatten_path(path2)
    shorter, longer = sorted((steps1, steps2), key=len)
    return tuple(longer[:len(shorter)]) == tuple(shorter)


def _canonical_operand(operand: _Operand) -> Any:
    if isinstance(operand, Path):
        return 'path', _path_segments(operand)
    if isinstance(operand, Value):
        return 'value', _canonical_dynamodb_value(operand.value)
    return (
        type(operand).__name__,
        tuple(_canonical_operand(value) for value in operand.values),
    )


def _canonical_action(action: Action) -> Tuple[Any, ...]:
    return (
        type(action).__name__,
        tuple(_canonical_operand(value) for value in action.values),
    )


def _action_paths(action: Action) -> List[Tuple[str, ...]]:
    return [
        _path_segments(value)
        for value in action.values
        if isinstance(value, Path)
    ]


def _is_idempotent_duplicate(action: Action) -> bool:
    """
    Returns whether applying ``action`` twice is equivalent to applying it
    once, so that identical duplicates (e.g. generated by different modules)
    may safely be collapsed into a single action.

    SET and REMOVE are idempotent. ADD on a set and DELETE of set elements are
    idempotent, whereas ADD on a number is not (it must only appear once).
    """
    if isinstance(action, (SetAction, RemoveAction)):
        return True
    if isinstance(action, (AddAction, DeleteAction)):
        value = action.values[1]
        return isinstance(value, Value) and value.attr_type in (BINARY_SET, NUMBER_SET, STRING_SET)
    return False


class _QueuedAction:
    """
    A write action enqueued on a merging TransactWrite, kept in its
    un-serialized form until the actions are merged at commit time.
    """

    def __init__(
        self,
        kind: str,
        index: int,
        table_name: str,
        key: Dict[str, Dict[str, Any]],
        location: Optional[_CallLocation],
        *,
        model_cls: Optional[Type[Model]] = None,
        model: Optional[Model] = None,
        actions: Optional[List[Action]] = None,
        condition: Optional[Condition] = None,
        add_version_condition: bool = True,
        return_values_on_condition_failure: Optional[str] = None,
        item: Optional[Dict[str, Any]] = None,
        raw_hash_key: Any = None,
        raw_range_key: Any = None,
    ) -> None:
        self.kind = kind
        self.index = index
        self.table_name = table_name
        self.key = key
        self.location = location
        self.model_cls = model_cls
        self.model = model
        self.actions = actions
        self.condition = condition
        self.add_version_condition = add_version_condition
        self.return_values_on_condition_failure = return_values_on_condition_failure
        self.item = item
        self.raw_hash_key = raw_hash_key
        self.raw_range_key = raw_range_key

    @property
    def identity(self) -> Tuple[str, Tuple]:
        return self.table_name, _canonical_key(self.key)

    def describe(self) -> str:
        location = ' at {}'.format(self.location) if self.location is not None else ''
        return '#{} ({}{} on {})'.format(
            self.index, self.kind, location, _describe_key(self.table_name, self.key)
        )


def _describe_key(table_name: str, key: Dict[str, Dict[str, Any]]) -> str:
    parts = ['{}={}'.format(name, value) for name, value in sorted(key.items())]
    return '{}/{}'.format(table_name, ', '.join(parts)) if parts else table_name


class Transaction:

    """
    Base class for a type of transaction operation
    """

    def __init__(self, connection: Connection, return_consumed_capacity: Optional[str] = None) -> None:
        self._connection = connection
        self._return_consumed_capacity = return_consumed_capacity

    def _commit(self):
        raise NotImplementedError()

    def __enter__(self: _TTransaction) -> _TTransaction:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None and exc_val is None and exc_tb is None:
            self._commit()


class TransactGet(Transaction):

    _results: Optional[List] = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._get_items: List[Dict] = []
        self._futures: List[_ModelFuture] = []
        super(TransactGet, self).__init__(*args, **kwargs)

    def get(self, model_cls: Type[_M], hash_key: _KeyType, range_key: Optional[_KeyType] = None) -> _ModelFuture[_M]:
        """
        Adds the operation arguments for an item to list of models to get
        returns a _ModelFuture object as a placeholder

        :param model_cls:
        :param hash_key:
        :param range_key:
        :return:
        """
        operation_kwargs = model_cls.get_operation_kwargs_from_class(hash_key, range_key=range_key)
        model_future = _ModelFuture(model_cls)
        self._futures.append(model_future)
        self._get_items.append(operation_kwargs)
        return model_future

    @staticmethod
    def _update_futures(futures: List[_ModelFuture], results: List) -> None:
        for model, data in zip(futures, results):
            model.update_with_raw_data(data.get(ITEM))

    def _commit(self) -> Any:
        response = self._connection.transact_get_items(
            get_items=self._get_items,
            return_consumed_capacity=self._return_consumed_capacity
        )

        results = response[RESPONSES]
        self._results = results
        self._update_futures(self._futures, results)
        return response


class TransactWrite(Transaction):

    def __init__(
        self,
        client_request_token: Optional[str] = None,
        return_item_collection_metrics: Optional[str] = None,
        merge_actions: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        :param client_request_token: an idempotency key for the request
        :param return_item_collection_metrics: whether to return item
            collection metrics
        :param merge_actions: when True, actions targeting the same DynamoDB
            item are merged into a single transaction action at commit time.
            Non-conflicting field updates are combined into one Update with
            their conditions AND-ed together. Conflicts (e.g. a delete and an
            update of the same item, or overlapping document paths) raise
            :class:`~pynamodb.exceptions.TransactWriteMergeError`.
        """
        super(TransactWrite, self).__init__(**kwargs)
        self._client_request_token: Optional[str] = client_request_token
        self._return_item_collection_metrics = return_item_collection_metrics
        self._merge_actions = merge_actions
        self._condition_check_items: List[Dict] = []
        self._delete_items: List[Dict] = []
        self._put_items: List[Dict] = []
        self._update_items: List[Dict] = []
        self._queued_actions: List[_QueuedAction] = []
        self._models_for_version_attribute_update: List[Any] = []
        self._serialized_identities: List[Tuple[str, Tuple[str, Tuple]]] = []

    def condition_check(self, model_cls: Type[_M], hash_key: _KeyType, range_key: Optional[_KeyType] = None, condition: Optional[Condition] = None):
        if condition is None:
            raise TypeError('`condition` cannot be None')
        if self._merge_actions:
            operation_kwargs = model_cls.get_operation_kwargs_from_class(hash_key, range_key=range_key)
            self._enqueue_merged_action(_QueuedAction(
                kind=_CONDITION_CHECK,
                index=len(self._queued_actions),
                table_name=operation_kwargs[TABLE_NAME],
                key=operation_kwargs[KEY],
                location=_capture_call_location(),
                model_cls=model_cls,
                condition=condition,
                raw_hash_key=hash_key,
                raw_range_key=range_key,
            ))
            return
        operation_kwargs = model_cls.get_operation_kwargs_from_class(
            hash_key,
            range_key=range_key,
            condition=condition
        )
        self._condition_check_items.append(operation_kwargs)
        self._serialized_identities.append(
            (_CONDITION_CHECK, (operation_kwargs[TABLE_NAME], _canonical_key(operation_kwargs[KEY])))
        )

    def delete(self, model: _M, condition: Optional[Condition] = None, *, add_version_condition: bool = True) -> None:
        if self._merge_actions:
            hash_key, range_key = model._get_hash_range_key_serialized_values()
            self._enqueue_merged_action(_QueuedAction(
                kind=_DELETE,
                index=len(self._queued_actions),
                table_name=model._get_connection().table_name,
                key=self._build_key(model, hash_key, range_key),
                location=_capture_call_location(),
                model=model,
                condition=condition,
                add_version_condition=add_version_condition,
            ))
            return
        operation_kwargs = model.get_delete_kwargs_from_instance(
            condition=condition,
            add_version_condition=add_version_condition,
        )
        self._delete_items.append(operation_kwargs)
        self._serialized_identities.append(
            (_DELETE, (operation_kwargs[TABLE_NAME], _canonical_key(operation_kwargs[KEY])))
        )

    def save(self, model: _M, condition: Optional[Condition] = None, return_values: Optional[str] = None) -> None:
        if self._merge_actions:
            args, save_kwargs = model._get_save_args(condition=condition)
            item = dict(save_kwargs['attributes'])
            hash_key = args[0]
            range_key = save_kwargs.get('range_key')
            hash_key_attribute = model._hash_key_attribute()
            item[hash_key_attribute.attr_name] = {hash_key_attribute.attr_type: hash_key}
            if range_key is not None:
                range_key_attribute = model._range_key_attribute()
                item[range_key_attribute.attr_name] = {range_key_attribute.attr_type: range_key}
            self._enqueue_merged_action(_QueuedAction(
                kind=_PUT,
                index=len(self._queued_actions),
                table_name=model._get_connection().table_name,
                key=self._build_key(model, hash_key, range_key),
                location=_capture_call_location(),
                model=model,
                condition=condition,
                return_values_on_condition_failure=return_values,
                item=item,
            ))
            self._models_for_version_attribute_update.append(model)
            return
        operation_kwargs = model.get_save_kwargs_from_instance(
            condition=condition,
            return_values_on_condition_failure=return_values
        )
        self._put_items.append(operation_kwargs)
        self._models_for_version_attribute_update.append(model)
        self._serialized_identities.append(
            (_PUT, (
                operation_kwargs[TABLE_NAME],
                _canonical_key(self._build_key_from_item(model, operation_kwargs[ITEM])),
            ))
        )

    def update(self, model: _M, actions: List[Action], condition: Optional[Condition] = None,
               return_values: Optional[str] = None,
               *,
               add_version_condition: bool = True) -> None:
        if self._merge_actions:
            if not isinstance(actions, list) or len(actions) == 0:
                raise TypeError("the value of `actions` is expected to be a non-empty list")
            hash_key, range_key = model._get_hash_range_key_serialized_values()
            self._enqueue_merged_action(_QueuedAction(
                kind=_UPDATE,
                index=len(self._queued_actions),
                table_name=model._get_connection().table_name,
                key=self._build_key(model, hash_key, range_key),
                location=_capture_call_location(),
                model=model,
                actions=list(actions),
                condition=condition,
                add_version_condition=add_version_condition,
                return_values_on_condition_failure=return_values,
            ))
            self._models_for_version_attribute_update.append(model)
            return
        operation_kwargs = model.get_update_kwargs_from_instance(
            actions=actions,
            condition=condition,
            return_values_on_condition_failure=return_values,
            add_version_condition=add_version_condition,
        )
        self._update_items.append(operation_kwargs)
        self._models_for_version_attribute_update.append(model)
        self._serialized_identities.append(
            (_UPDATE, (operation_kwargs[TABLE_NAME], _canonical_key(operation_kwargs[KEY])))
        )

    def _enqueue_merged_action(self, queued_action: _QueuedAction) -> None:
        self._queued_actions.append(queued_action)

    @staticmethod
    def _build_key(model: Model, hash_key: Any, range_key: Optional[Any]) -> Dict[str, Dict[str, Any]]:
        key = {model._hash_key_attribute().attr_name: {model._hash_key_attribute().attr_type: hash_key}}
        range_key_attribute = model._range_key_attribute()
        if range_key is not None and range_key_attribute is not None:
            key[range_key_attribute.attr_name] = {range_key_attribute.attr_type: range_key}
        return key

    @staticmethod
    def _build_key_from_item(model: Model, item: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        key = {model._hash_key_attribute().attr_name: item[model._hash_key_attribute().attr_name]}
        range_key_attribute = model._range_key_attribute()
        if range_key_attribute is not None and range_key_attribute.attr_name in item:
            key[range_key_attribute.attr_name] = item[range_key_attribute.attr_name]
        return key

    def _commit(self) -> Any:
        if self._merge_actions:
            self._commit_merged()
        response = self._connection.transact_write_items(
            condition_check_items=self._condition_check_items,
            delete_items=self._delete_items,
            put_items=self._put_items,
            update_items=self._update_items,
            client_request_token=self._client_request_token,
            return_consumed_capacity=self._return_consumed_capacity,
            return_item_collection_metrics=self._return_item_collection_metrics,
        )
        for model in self._unique_models(self._models_for_version_attribute_update):
            model.update_local_version_attribute()
        return response

    @staticmethod
    def _unique_models(models: List[Model]) -> List[Model]:
        unique_models: List[Model] = []
        seen_ids = set()
        for model in models:
            if id(model) not in seen_ids:
                seen_ids.add(id(model))
                unique_models.append(model)
        return unique_models

    def _assert_items_unique(self) -> None:
        """
        Verifies that the final request contains at most one action per item.
        This is an internal invariant check for the merge; actionable merge
        conflicts (with original action positions) are reported earlier.
        """
        seen = set()
        duplicates = []
        for kind, identity in self._serialized_identities:
            if identity in seen:
                duplicates.append((kind, identity))
            seen.add(identity)
        if duplicates:
            raise TransactWriteMergeError(
                'The merged transaction unexpectedly contains multiple actions for {} item(s): {}'
                .format(len(duplicates), duplicates)
            )

    def _commit_merged(self) -> None:
        groups: "OrderedDict[Tuple[str, Tuple], List[_QueuedAction]]" = OrderedDict()
        for queued_action in self._queued_actions:
            groups.setdefault(queued_action.identity, []).append(queued_action)

        conflicts: List[str] = []
        merged_items: List[Tuple[str, Tuple, Dict]] = []
        for group in groups.values():
            kind, operation_kwargs, group_conflicts = self._merge_group(group)
            conflicts.extend(group_conflicts)
            if operation_kwargs is None:
                continue
            merged_items.append((kind, group[0].identity, operation_kwargs))

        if conflicts:
            raise TransactWriteMergeError(
                'Cannot merge the following transaction action(s):\n' + '\n'.join(conflicts)
            )

        for kind, identity, operation_kwargs in merged_items:
            self._serialized_identities.append((kind, identity))
            if kind == _CONDITION_CHECK:
                self._condition_check_items.append(operation_kwargs)
            elif kind == _DELETE:
                self._delete_items.append(operation_kwargs)
            elif kind == _PUT:
                self._put_items.append(operation_kwargs)
            else:
                self._update_items.append(operation_kwargs)

        self._assert_items_unique()

    def _merge_group(self, group: List[_QueuedAction]) -> Tuple[str, Optional[Dict], List[str]]:
        """
        Merges all actions targeting a single item into at most one
        transaction action. Returns the resulting kind, the serialized kwargs
        (or None on conflict), and human-readable conflict messages.
        """
        conflicts: List[str] = []
        kinds = {queued_action.kind for queued_action in group}
        return_values = self._merge_return_values(group, conflicts)

        if _PUT in kinds:
            return self._merge_put_group(group, return_values, conflicts)

        if _DELETE in kinds:
            if _UPDATE in kinds:
                self._add_conflict(
                    conflicts, group,
                    'cannot merge a delete and an update of the same item'
                )
                return _DELETE, None, conflicts
            version_conflicts = self._check_version_expectations(
                [queued_action for queued_action in group if queued_action.kind == _DELETE]
            )
            conflicts.extend(version_conflicts)
            if version_conflicts:
                return _DELETE, None, conflicts
            return _DELETE, self._build_delete_kwargs(group, return_values), conflicts

        update_records = [queued_action for queued_action in group if queued_action.kind == _UPDATE]
        if update_records:
            merged_actions, action_conflicts = self._merge_update_actions(update_records)
            conflicts.extend(action_conflicts)
            version_conflicts = self._check_version_expectations(update_records)
            conflicts.extend(version_conflicts)
            if action_conflicts or version_conflicts:
                return _UPDATE, None, conflicts
            merged_condition = self._merge_conditions(group)
            return (
                _UPDATE,
                self._build_update_kwargs(update_records, merged_actions, merged_condition, return_values),
                conflicts,
            )

        first = group[0]
        operation_kwargs = first.model_cls.get_operation_kwargs_from_class(
            first.raw_hash_key,
            range_key=first.raw_range_key,
            condition=self._merge_conditions(group),
        )
        return _CONDITION_CHECK, operation_kwargs, conflicts

    def _merge_put_group(
        self,
        group: List[_QueuedAction],
        return_values: Optional[str],
        conflicts: List[str],
    ) -> Tuple[str, Optional[Dict], List[str]]:
        non_put = [
            queued_action for queued_action in group
            if queued_action.kind not in (_PUT, _CONDITION_CHECK)
        ]
        if non_put:
            self._add_conflict(
                conflicts, group,
                'cannot merge a put with {} action(s) on the same item'.format(
                    ', '.join(sorted({queued_action.kind for queued_action in non_put}))
                )
            )
            return _PUT, None, conflicts

        version_conflicts = self._check_version_expectations(
            [queued_action for queued_action in group if queued_action.kind == _PUT]
        )
        conflicts.extend(version_conflicts)

        put_records = [queued_action for queued_action in group if queued_action.kind == _PUT]
        first = put_records[0]
        for queued_action in put_records[1:]:
            if _canonical_item(queued_action.item) != _canonical_item(first.item):
                self._add_conflict(
                    conflicts, [first, queued_action],
                    'cannot merge puts that write different attribute values for the same item'
                )
                return _PUT, None, conflicts
        if version_conflicts:
            return _PUT, None, conflicts

        merged_condition = self._merge_conditions(group)
        version_condition = first.model._handle_version_attribute()
        if version_condition is not None:
            merged_condition &= version_condition
        hash_key, range_key = self._key_parts(first.model, first.key)
        operation_kwargs = first.model._get_connection().get_operation_kwargs(
            hash_key=hash_key,
            range_key=range_key,
            key=ITEM,
            attributes={
                attr_name: attr_value
                for attr_name, attr_value in first.item.items()
                if attr_name not in first.key
            },
            condition=merged_condition,
            return_values_on_condition_failure=return_values,
        )
        return _PUT, operation_kwargs, conflicts

    def _merge_update_actions(
        self,
        update_records: List[_QueuedAction],
    ) -> Tuple[List[Action], List[str]]:
        """
        Combines the update actions of every record for a single item,
        collapsing identical idempotent actions and rejecting overlapping
        document paths (including parent/child paths) and duplicate
        non-idempotent actions.
        """
        conflicts: List[str] = []
        merged: List[Action] = []
        merged_canonical: List[Tuple[Any, ...]] = []
        merged_owners: List[_QueuedAction] = []

        for queued_action in update_records:
            for action in queued_action.actions:
                canonical = _canonical_action(action)
                try:
                    duplicate_index = merged_canonical.index(canonical)
                except ValueError:
                    duplicate_index = -1

                if duplicate_index >= 0:
                    if not _is_idempotent_duplicate(action):
                        conflicts.append(
                            'cannot merge duplicate {} action {!r} from {} and {}'.format(
                                type(action).__name__,
                                action,
                                merged_owners[duplicate_index].describe(),
                                queued_action.describe(),
                            )
                        )
                    continue

                for existing_action, owner in zip(merged, merged_owners):
                    if _actions_conflict_on_paths(existing_action, action):
                        conflicts.append(
                            'document paths of a {} action from {} and a {} action from {} conflict'.format(
                                type(existing_action).__name__,
                                owner.describe(),
                                type(action).__name__,
                                queued_action.describe(),
                            )
                        )

                merged.append(action)
                merged_canonical.append(canonical)
                merged_owners.append(queued_action)

        return merged, _unique_in_order(conflicts)

    @staticmethod
    def _merge_conditions(records: List[_QueuedAction]) -> Optional[Condition]:
        condition: Optional[Condition] = None
        for queued_action in records:
            if queued_action.condition is None:
                continue
            condition = queued_action.condition if condition is None else condition & queued_action.condition
        return condition

    @staticmethod
    def _merge_return_values(group: List[_QueuedAction], conflicts: List[str]) -> Optional[str]:
        return_values = {
            queued_action.return_values_on_condition_failure
            for queued_action in group
            if queued_action.return_values_on_condition_failure is not None
        }
        if len(return_values) > 1:
            TransactWrite._add_conflict(
                conflicts, group,
                'incompatible ReturnValuesOnConditionCheckFailure values: {}'.format(
                    ', '.join(sorted(return_values))
                )
            )
            return None
        return next(iter(return_values), None)

    @staticmethod
    def _add_conflict(conflicts: List[str], group: List[_QueuedAction], message: str) -> None:
        positions = ' and '.join(queued_action.describe() for queued_action in group)
        conflicts.append('{}: {}'.format(message, positions))

    @staticmethod
    def _key_parts(model: Model, key: Dict[str, Dict[str, Any]]) -> Tuple[Any, Optional[Any]]:
        """
        Returns the serialized (hash_key, range_key) scalar values for the
        given serialized key map, in the model's key schema order.
        """
        hash_value = next(iter(key[model._hash_key_attribute().attr_name].values()))
        range_value = None
        range_key_attribute = model._range_key_attribute()
        if range_key_attribute is not None and range_key_attribute.attr_name in key:
            range_value = next(iter(key[range_key_attribute.attr_name].values()))
        return hash_value, range_value

    def _check_version_expectations(self, records: List[_QueuedAction]) -> List[str]:
        """
        Rejects merging actions whose optimistic-locking version conditions
        are not equivalent (e.g. two updates built from model instances that
        observed different versions of the same item).
        """
        conflicts: List[str] = []
        expected: Optional[Tuple[Any, ...]] = None
        expected_record: Optional[_QueuedAction] = None
        for queued_action in records:
            if not queued_action.add_version_condition:
                continue
            version_name = queued_action.model._version_attribute_name
            if version_name is None:
                continue
            version_value = getattr(queued_action.model, version_name)
            signature = (version_name, version_value)
            if expected is None:
                expected = signature
                expected_record = queued_action
            elif signature != expected:
                conflicts.append(
                    'incompatible optimistic-locking version conditions '
                    '(expected {} from {} vs {} from {}): {}'.format(
                        expected[1],
                        expected_record.describe(),
                        version_value,
                        queued_action.describe(),
                        ' and '.join(record.describe() for record in records),
                    )
                )
        return _unique_in_order(conflicts)

    def _build_update_kwargs(
        self,
        update_records: List[_QueuedAction],
        merged_actions: List[Action],
        condition: Optional[Condition],
        return_values: Optional[str],
    ) -> Dict:
        first = update_records[0]
        model = first.model

        # Apply the optimistic-locking bookkeeping once for the whole group.
        version_actions: List[Action] = []
        version_condition = model._handle_version_attribute(actions=version_actions)
        for version_action in version_actions:
            version_canonical = _canonical_action(version_action)
            if all(_canonical_action(action) != version_canonical for action in merged_actions):
                merged_actions.append(version_action)
        if version_condition is not None and all(
            queued_action.add_version_condition for queued_action in update_records
        ):
            condition = version_condition if condition is None else condition & version_condition

        hash_key, range_key = self._key_parts(model, first.key)
        return model._get_connection().get_operation_kwargs(
            hash_key=hash_key,
            range_key=range_key,
            key=KEY,
            actions=merged_actions,
            condition=condition,
            return_values_on_condition_failure=return_values,
        )

    def _build_delete_kwargs(self, group: List[_QueuedAction], return_values: Optional[str]) -> Dict:
        delete_records = [queued_action for queued_action in group if queued_action.kind == _DELETE]
        first = delete_records[0]
        model = first.model
        condition = self._merge_conditions(group)
        version_condition = model._handle_version_attribute()
        if version_condition is not None and all(
            queued_action.add_version_condition for queued_action in delete_records
        ):
            condition = version_condition if condition is None else condition & version_condition
        hash_key, range_key = self._key_parts(model, first.key)
        return model._get_connection().get_operation_kwargs(
            hash_key=hash_key,
            range_key=range_key,
            key=KEY,
            condition=condition,
            return_values_on_condition_failure=return_values,
        )


def _unique_in_order(values: List[str]) -> List[str]:
    unique: List[str] = []
    seen = set()
    for value in values:
        if value not in seen:
            seen.add(value)
            unique.append(value)
    return unique


def _actions_conflict_on_paths(existing: Action, candidate: Action) -> bool:
    """
    Paths conflict when any document path referenced by the two actions is
    identical or in a parent/child relationship. DynamoDB rejects such paths
    appearing multiple times in a single update expression, even across
    different SET/REMOVE/ADD/DELETE clauses.
    """
    existing_paths = _action_paths(existing)
    candidate_paths = _action_paths(candidate)
    return any(
        _paths_overlap(existing_path, candidate_path)
        for existing_path in existing_paths
        for candidate_path in candidate_paths
    )


def _canonical_item(item: Optional[Dict[str, Any]]) -> Any:
    if item is None:
        return None
    return tuple(sorted(
        (attr_name, _canonical_dynamodb_value(attr_value))
        for attr_name, attr_value in item.items()
    ))
