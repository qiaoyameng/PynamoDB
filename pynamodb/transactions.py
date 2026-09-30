import inspect
import re
from collections import OrderedDict
from typing import Tuple, TypeVar, Type, Any, List, Optional, Dict, Union, Text, Generic
from typing import TYPE_CHECKING

from pynamodb.connection import Connection
from pynamodb.constants import ITEM, KEY, RESPONSES, TABLE_NAME
from pynamodb.exceptions import MergedActionLocation, TransactWriteMergeError
from pynamodb.expressions.condition import And, Condition
from pynamodb.expressions.operand import Path, _Operand
from pynamodb.expressions.update import Action
from pynamodb.expressions.util import PATH_SEGMENT_REGEX
from pynamodb.models import Model, _ModelFuture, _KeyType

if TYPE_CHECKING:
    from pynamodb.attributes import Attribute

_M = TypeVar('_M', bound=Model)
_TTransaction = TypeVar('_TTransaction', bound='Transaction')

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



# ---------------------------------------------------------------------------
# Optional merge-phase helpers
# ---------------------------------------------------------------------------

# Operation kinds, ordered to match the wire order used by TransactWriteItems.
_CONDITION_CHECK = 'condition_check'
_DELETE = 'delete'
_PUT = 'put'
_UPDATE = 'update'

# A normalized document-path segment: (attribute name, tuple of list indices).
PathSegment = Tuple[str, Tuple[int, ...]]


class CallerLocation:
    """
    Lightweight snapshot of where a transaction action was added from.
    Only immutable metadata is retained (unlike a frame object, which would
    keep the caller's locals alive).
    """

    __slots__ = ('filename', 'lineno', 'function')

    def __init__(self, filename: str, lineno: int, function: str) -> None:
        self.filename = filename
        self.lineno = lineno
        self.function = function


def _get_caller_location() -> Optional[CallerLocation]:
    """
    Returns the location of the first caller outside of this module.
    """
    frame = inspect.currentframe()
    while frame is not None:
        if frame.f_code.co_filename != __file__:
            location = CallerLocation(
                frame.f_code.co_filename, frame.f_lineno, frame.f_code.co_name,
            )
            del frame
            return location
        frame = frame.f_back
    return None


def _normalize_path(path: Path) -> Tuple[PathSegment, ...]:
    """
    Normalizes a document path (resolving attribute aliases to physical names)
    into a tuple of (name, list-indices) segments.
    """
    segments: List[PathSegment] = []
    for raw_segment in path.path:
        match = PATH_SEGMENT_REGEX.match(raw_segment)
        if not match:
            raise ValueError('{} is not a valid document path'.format('.'.join(path.path)))
        name, indexes = match.groups()
        index_tuple: Tuple[int, ...] = ()
        if indexes:
            index_tuple = tuple(int(part) for part in re.findall(r'\[(\d+)\]', indexes))
        segments.append((name, index_tuple))
    return tuple(segments)


def _is_ancestor_path(ancestor: Tuple[PathSegment, ...], descendant: Tuple[PathSegment, ...]) -> bool:
    """
    True if `ancestor` is a strict ancestor of, or equal to, `descendant`.
    """
    return descendant[:len(ancestor)] == ancestor


def _collect_operand_paths(operand: Any) -> List[Tuple[PathSegment, ...]]:
    """
    Recursively collects every document path referenced inside an expression
    operand, including paths nested inside list_append/increment/etc.
    """
    if isinstance(operand, Path):
        return [_normalize_path(operand)]
    paths: List[Tuple[PathSegment, ...]] = []
    if isinstance(operand, _Operand):
        for value in operand.values:
            paths.extend(_collect_operand_paths(value))
    return paths


def _action_target_path(action: Action) -> Tuple[PathSegment, ...]:
    return _normalize_path(action.values[0])


def _action_read_paths(action: Action) -> List[Tuple[PathSegment, ...]]:
    paths: List[PathSegment, ...] = []
    for operand in action.values[1:]:
        paths.extend(_collect_operand_paths(operand))
    return paths


def _paths_conflict(path1: Tuple[PathSegment, ...], path2: Tuple[PathSegment, ...]) -> bool:
    """
    Two document paths conflict if they are equal or one is a parent (ancestor)
    of the other; such parent/child updates cannot be combined in one Update.
    """
    return _is_ancestor_path(path1, path2) or _is_ancestor_path(path2, path1)


def _add_unique_condition(conditions: List[Condition], condition: Optional[Condition]) -> None:
    if condition is not None and not any(condition == existing for existing in conditions):
        conditions.append(condition)


def _and_conditions(conditions: List[Condition]) -> Optional[Condition]:
    merged: Optional[Condition] = None
    for condition in conditions:
        merged = condition if merged is None else And(merged, condition)
    return merged


def _merge_actions(existing: List[Action], incoming: List[Action]) -> None:
    """
    Merges the ``incoming`` actions into ``existing`` in place.

    Identical actions are de-duplicated. Any two non-identical actions whose
    document paths overlap (equal paths or a parent/child relation, either as
    write targets or as paths read by the other action) cannot be expressed
    equivalently within a single UpdateItem and raise a merge error.
    """
    for new_action in incoming:
        new_target = _action_target_path(new_action)
        for old_action in existing:
            old_target = _action_target_path(old_action)
            if new_action == old_action:
                break
            if _paths_conflict(old_target, new_target):
                raise TransactWriteMergeError(
                    "Cannot merge update actions that both write the same or a "
                    "parent/child document path: '{}' and '{}'".format(
                        '.'.join(segment[0] for segment in old_target),
                        '.'.join(segment[0] for segment in new_target),
                    ),
                    [],
                )
            if any(_paths_conflict(new_target, path) for path in _action_read_paths(old_action)):
                raise TransactWriteMergeError(
                    "Cannot merge update actions where one writes a path read by "
                    "the other (parent/child conflict): '{}' and '{}'".format(
                        '.'.join(segment[0] for segment in new_target),
                        '.'.join(segment[0] for segment in old_target),
                    ),
                    [],
                )
            if any(_paths_conflict(old_target, path) for path in _action_read_paths(new_action)):
                raise TransactWriteMergeError(
                    "Cannot merge update actions where one writes a path read by "
                    "the other (parent/child conflict): '{}' and '{}'".format(
                        '.'.join(segment[0] for segment in old_target),
                        '.'.join(segment[0] for segment in new_target),
                    ),
                    [],
                )
        else:
            existing.append(new_action)


class _PendingAction:
    """
    A single condition_check/delete/save/update call awaiting commit.
    """

    def __init__(
        self,
        kind: str,
        model_cls: Type[Model],
        item_key: Tuple,
        source: Optional[CallerLocation],
        model: Optional[Model] = None,
        condition: Optional[Condition] = None,
        actions: Optional[List[Action]] = None,
        return_values: Optional[str] = None,
        version_condition: Optional[Condition] = None,
        version_attribute: Optional['Attribute'] = None,
        add_version_condition: bool = True,
        put_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.kind = kind
        self.model_cls = model_cls
        self.item_key = item_key
        self.source = source
        self.model = model
        self.condition = condition
        self.actions = actions
        self.return_values = return_values
        self.version_condition = version_condition
        self.version_attribute = version_attribute
        self.add_version_condition = add_version_condition
        self.put_kwargs = put_kwargs

    @property
    def effective_version_condition(self) -> Optional[Condition]:
        return self.version_condition if self.add_version_condition else None

    def location(self) -> MergedActionLocation:
        table_name, hash_part, range_part = self.item_key
        hash_name, _, hash_value = hash_part
        item = "{}/{}={}".format(table_name, hash_name, hash_value)
        if range_part is not None:
            range_name, _, range_value = range_part
            item += ", {}={}".format(range_name, range_value)
        return MergedActionLocation(
            operation=self.kind,
            filename=self.source.filename if self.source else None,
            lineno=self.source.lineno if self.source else None,
            function=self.source.function if self.source else None,
            item=item,
        )


class _MergedGroup:
    """
    The result of merging every pending action that targets one DynamoDB item.
    """

    def __init__(self, first: _PendingAction) -> None:
        self.key = first.item_key
        self.pending: List[_PendingAction] = [first]
        self.conditions: List[Condition] = []
        self.return_values: Optional[str] = None
        self.version_condition: Optional[Condition] = None
        self.version_attribute: Optional['Attribute'] = None
        self.kind = first.kind
        self.actions: List[Action] = list(first.actions or []) if first.kind == _UPDATE else []
        self.put_kwargs: Optional[Dict[str, Any]] = first.put_kwargs if first.kind == _PUT else None
        self._absorb(first)

    @property
    def writes(self) -> List[_PendingAction]:
        return [pending for pending in self.pending if pending.kind != _CONDITION_CHECK]

    def _reject(self, message: str, incoming: Optional[_PendingAction] = None) -> None:
        actions = [pending.location() for pending in self.pending]
        if incoming is not None:
            actions.append(incoming.location())
        raise TransactWriteMergeError(message, actions)

    def _absorb(self, pending: _PendingAction) -> None:
        _add_unique_condition(self.conditions, pending.condition)

        if pending.version_attribute is not None:
            self.version_attribute = pending.version_attribute
        effective_version_condition = pending.effective_version_condition
        if effective_version_condition is not None:
            if (
                self.version_condition is not None
                and effective_version_condition != self.version_condition
            ):
                self._reject(
                    "Cannot merge actions with conflicting optimistic-locking version "
                    "conditions on the same item",
                    pending if pending not in self.pending else None,
                )
            self.version_condition = effective_version_condition

        if pending.return_values is not None:
            if self.return_values is not None and pending.return_values != self.return_values:
                self._reject(
                    "Cannot merge actions with different "
                    "ReturnValuesOnConditionCheckFailure values "
                    "('{}' and '{}')".format(self.return_values, pending.return_values),
                    pending if pending not in self.pending else None,
                )
            self.return_values = pending.return_values

    def absorb(self, pending: _PendingAction) -> None:
        existing_writes = self.writes
        if existing_writes:
            existing_kind = existing_writes[0].kind
            if pending.kind == _CONDITION_CHECK:
                pass
            elif pending.kind != existing_kind:
                self._reject(
                    "Cannot merge a '{}' action with a '{}' action on the same item".format(
                        existing_kind, pending.kind,
                    ),
                    pending,
                )
            elif existing_kind == _PUT:
                incoming_kwargs = pending.put_kwargs
                if incoming_kwargs != self.put_kwargs:
                    self._reject(
                        "Cannot merge non-equivalent 'put' actions on the same item",
                        pending,
                    )
            elif existing_kind == _UPDATE:
                merged_actions = list(self.actions)
                try:
                    _merge_actions(merged_actions, pending.actions or [])
                except TransactWriteMergeError as error:
                    self._reject(error.msg, pending)
                self.actions = merged_actions
        elif pending.kind != _CONDITION_CHECK:
            self.kind = pending.kind
            if pending.kind == _PUT:
                self.put_kwargs = pending.put_kwargs
            elif pending.kind == _UPDATE:
                self.actions = list(pending.actions or [])

        self.pending.append(pending)
        self._absorb(pending)

    @staticmethod
    def _key_attribute_names(model_cls: Type[Model]) -> List[str]:
        names = [model_cls._hash_key_attribute().attr_name]
        range_key_attribute = model_cls._range_key_attribute()
        if range_key_attribute is not None:
            names.append(range_key_attribute.attr_name)
        return names

    def _condition(self) -> Optional[Condition]:
        condition = _and_conditions(self.conditions)
        if self.version_condition is not None:
            condition = (
                self.version_condition if condition is None
                else And(condition, self.version_condition)
            )
        return condition

    def _local_version_models(self) -> List[Model]:
        models: List[Model] = []
        for pending in self.pending:
            if pending.kind in (_PUT, _UPDATE) and pending.model is not None:
                if not any(existing is pending.model for existing in models):
                    models.append(pending.model)
        return models

    def serialize(self) -> Dict[str, Any]:
        condition = self._condition()
        representative = self.writes[0] if self.writes else self.pending[0]
        model_cls = representative.model_cls
        hash_value = self.key[1][2]
        range_value = self.key[2][2] if self.key[2] is not None else None

        if self.kind == _CONDITION_CHECK:
            return model_cls.get_operation_kwargs_from_class(
                hash_value,
                range_key=range_value,
                condition=condition,
            )
        if self.kind == _DELETE:
            model = representative.model
            assert model is not None
            return model.get_delete_kwargs_from_instance(
                condition=condition,
                return_values_on_condition_failure=self.return_values,
                add_version_condition=False,
            )
        if self.kind == _PUT:
            # Equivalent puts share identical, independently-serialized kwargs
            # (including the folded-in version condition); rebuild them with
            # the fully merged condition so condition_check actions are AND-ed.
            assert representative.put_kwargs is not None
            return representative.model_cls._get_connection().get_operation_kwargs(
                hash_value,
                range_key=range_value,
                key=ITEM,
                attributes={
                    name: value
                    for name, value in representative.put_kwargs[ITEM].items()
                    if name not in self._key_attribute_names(model_cls)
                },
                condition=condition,
                return_values_on_condition_failure=self.return_values,
            )
        # _UPDATE
        model = representative.model
        assert model is not None
        actions = list(self.actions)
        # The version attribute is always incremented on update (even when the
        # optimistic-locking version *condition* is disabled), matching the
        # behavior of ``Model.update`` / ``_handle_version_attribute``.
        if self.version_attribute is not None:
            version_value = getattr(model, self.version_attribute.attr_name)
            if version_value is not None:
                actions.append(self.version_attribute.add(1))
            else:
                actions.append(self.version_attribute.set(1))
        return model._get_connection().get_operation_kwargs(
            hash_value,
            range_key=range_value,
            key=KEY,
            actions=actions,
            condition=condition,
            return_values_on_condition_failure=self.return_values,
        )


class TransactWrite(Transaction):
    """
    A write transaction.

    :param merge: when ``True``, an optional merge (coalescing) phase runs at
        commit time. Multiple actions targeting the same DynamoDB item are
        combined into a single transaction entry: non-conflicting ``update``
        actions are merged into one ``UpdateItem`` (with their conditions
        combined with ``AND``), and ``condition_check`` actions are folded into
        the condition of the item's write. Actions that cannot be merged
        equivalently (delete-vs-update on the same item, parent/child update
        paths, non-equivalent puts, or contradictory version conditions) raise
        a :class:`~pynamodb.exceptions.TransactWriteMergeError` before any
        request is sent.
    """

    def __init__(
        self,
        client_request_token: Optional[str] = None,
        return_item_collection_metrics: Optional[str] = None,
        merge: bool = False,
        **kwargs: Any,
    ) -> None:
        super(TransactWrite, self).__init__(**kwargs)
        self._client_request_token: Optional[str] = client_request_token
        self._return_item_collection_metrics = return_item_collection_metrics
        self._merge_enabled = merge
        self._condition_check_items: List[Dict] = []
        self._delete_items: List[Dict] = []
        self._put_items: List[Dict] = []
        self._update_items: List[Dict] = []
        self._models_for_version_attribute_update: List[Any] = []
        self._pending_actions: List[_PendingAction] = []

    @staticmethod
    def _item_key(model_cls: Type[Model], serialized_hash_key: Any, serialized_range_key: Any) -> Tuple:
        table = model_cls._get_connection().get_meta_table()
        hash_name = table.hash_keyname
        range_name = table.range_keyname
        hash_type = table.get_attribute_type(hash_name)
        hash_part = (hash_name, hash_type, serialized_hash_key)
        range_part = None
        if serialized_range_key is not None and range_name is not None:
            range_type = table.get_attribute_type(range_name)
            range_part = (range_name, range_type, serialized_range_key)
        return model_cls.Meta.table_name, hash_part, range_part

    def _add_pending(self, pending: _PendingAction) -> None:
        self._pending_actions.append(pending)

    def condition_check(
        self,
        model_cls: Type[_M],
        hash_key: _KeyType,
        range_key: Optional[_KeyType] = None,
        condition: Optional[Condition] = None,
    ):
        if condition is None:
            raise TypeError('`condition` cannot be None')
        serialized_hash_key, serialized_range_key = model_cls._serialize_keys(hash_key, range_key)
        item_key = self._item_key(model_cls, serialized_hash_key, serialized_range_key)
        if not self._merge_enabled:
            operation_kwargs = model_cls.get_operation_kwargs_from_class(
                hash_key,
                range_key=range_key,
                condition=condition
            )
            self._condition_check_items.append(operation_kwargs)
            return
        self._add_pending(_PendingAction(
            kind=_CONDITION_CHECK,
            model_cls=model_cls,
            item_key=item_key,
            source=_get_caller_location(),
            condition=condition,
        ))

    def delete(self, model: _M, condition: Optional[Condition] = None, *, add_version_condition: bool = True) -> None:
        version_condition, version_attribute = self._version_info(model)
        if not self._merge_enabled:
            operation_kwargs = model.get_delete_kwargs_from_instance(
                condition=condition,
                add_version_condition=add_version_condition,
            )
            self._delete_items.append(operation_kwargs)
            return
        hk_value, rk_value = model._get_hash_range_key_serialized_values()
        item_key = self._item_key(type(model), hk_value, rk_value)
        self._add_pending(_PendingAction(
            kind=_DELETE,
            model_cls=type(model),
            item_key=item_key,
            source=_get_caller_location(),
            model=model,
            condition=condition,
            version_condition=version_condition,
            version_attribute=version_attribute,
            add_version_condition=add_version_condition,
        ))

    def save(self, model: _M, condition: Optional[Condition] = None, return_values: Optional[str] = None) -> None:
        version_condition, version_attribute = self._version_info(model)
        operation_kwargs = model.get_save_kwargs_from_instance(
            condition=condition,
            return_values_on_condition_failure=return_values
        )
        if not self._merge_enabled:
            self._put_items.append(operation_kwargs)
            self._models_for_version_attribute_update.append(model)
            return
        hk_value, rk_value = model._get_hash_range_key_serialized_values()
        item_key = self._item_key(type(model), hk_value, rk_value)
        self._add_pending(_PendingAction(
            kind=_PUT,
            model_cls=type(model),
            item_key=item_key,
            source=_get_caller_location(),
            model=model,
            condition=condition,
            return_values=return_values,
            version_condition=version_condition,
            version_attribute=version_attribute,
            put_kwargs=operation_kwargs,
        ))

    def update(self, model: _M, actions: List[Action], condition: Optional[Condition] = None,
               return_values: Optional[str] = None,
               *,
               add_version_condition: bool = True) -> None:
        version_condition, version_attribute = self._version_info(model, actions=actions)
        if not self._merge_enabled:
            operation_kwargs = model.get_update_kwargs_from_instance(
                actions=actions,
                condition=condition,
                return_values_on_condition_failure=return_values,
                add_version_condition=add_version_condition,
            )
            self._update_items.append(operation_kwargs)
            self._models_for_version_attribute_update.append(model)
            return
        user_actions = list(actions)
        hk_value, rk_value = model._get_hash_range_key_serialized_values()
        item_key = self._item_key(type(model), hk_value, rk_value)
        self._add_pending(_PendingAction(
            kind=_UPDATE,
            model_cls=type(model),
            item_key=item_key,
            source=_get_caller_location(),
            model=model,
            condition=condition,
            actions=user_actions,
            return_values=return_values,
            version_condition=version_condition,
            version_attribute=version_attribute,
            add_version_condition=add_version_condition,
        ))

    @staticmethod
    def _version_info(model: Model, *, actions: Optional[List[Action]] = None):
        """
        Returns the optimistic-locking (condition, attribute) pair without
        mutating the actions list (unlike ``_handle_version_attribute``).
        """
        if model._version_attribute_name is None:
            return None, None
        version_attribute = model.get_attributes()[model._version_attribute_name]
        value = getattr(model, model._version_attribute_name)
        if value is not None:
            return version_attribute == value, version_attribute
        return version_attribute.does_not_exist(), version_attribute

    def _merge_pending_actions(self) -> None:
        groups: "OrderedDict[Tuple, _MergedGroup]" = OrderedDict()
        for pending in self._pending_actions:
            group = groups.get(pending.item_key)
            if group is None:
                groups[pending.item_key] = _MergedGroup(pending)
            else:
                group.absorb(pending)

        for group in groups.values():
            item_kwargs = group.serialize()
            if group.kind == _CONDITION_CHECK:
                self._condition_check_items.append(item_kwargs)
            elif group.kind == _DELETE:
                self._delete_items.append(item_kwargs)
            elif group.kind == _PUT:
                self._put_items.append(item_kwargs)
            else:
                self._update_items.append(item_kwargs)
            for model in group._local_version_models():
                self._models_for_version_attribute_update.append(model)

    def _assert_each_item_appears_once(self) -> None:
        seen: Dict[Tuple, str] = {}

        def register(item_kwargs: Dict[str, Any], kind: str) -> None:
            if kind == _PUT:
                item = item_kwargs[ITEM]
                identity = tuple((name, tuple(sorted(value.items())))
                                 for name, value in sorted(item.items()))
            else:
                key = item_kwargs[KEY]
                identity = tuple((name, tuple(sorted(value.items())))
                                 for name, value in sorted(key.items()))
            key_tuple = (item_kwargs[TABLE_NAME], identity)
            previous = seen.get(key_tuple)
            if previous is not None:
                raise TransactWriteMergeError(
                    "Item {} appears more than once in the merged transaction "
                    "(actions '{}' and '{}')".format(
                        item_kwargs[TABLE_NAME], previous, kind,
                    ),
                    [],
                )
            seen[key_tuple] = kind

        for item in self._condition_check_items:
            register(item, _CONDITION_CHECK)
        for item in self._delete_items:
            register(item, _DELETE)
        for item in self._put_items:
            register(item, _PUT)
        for item in self._update_items:
            register(item, _UPDATE)

    def _commit(self) -> Any:
        if self._merge_enabled:
            self._merge_pending_actions()
            self._assert_each_item_appears_once()

        response = self._connection.transact_write_items(
            condition_check_items=self._condition_check_items,
            delete_items=self._delete_items,
            put_items=self._put_items,
            update_items=self._update_items,
            client_request_token=self._client_request_token,
            return_consumed_capacity=self._return_consumed_capacity,
            return_item_collection_metrics=self._return_item_collection_metrics,
        )
        for model in self._models_for_version_attribute_update:
            model.update_local_version_attribute()
        return response
