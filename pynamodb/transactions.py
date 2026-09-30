import logging
from typing import Tuple, TypeVar, Type, Any, List, Optional, Dict, Union, Text, Generic

from pynamodb.connection import Connection
from pynamodb.constants import ITEM, RESPONSES
from pynamodb.expressions.condition import Condition
from pynamodb.expressions.update import Action
from pynamodb.exceptions import TransactWriteError
from pynamodb.models import Model, _ModelFuture, _KeyType

_M = TypeVar('_M', bound=Model)
_TTransaction = TypeVar('_TTransaction', bound='Transaction')

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


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
        **kwargs: Any,
    ) -> None:
        super(TransactWrite, self).__init__(**kwargs)
        self._client_request_token: Optional[str] = client_request_token
        self._return_item_collection_metrics = return_item_collection_metrics
        self._condition_check_items: List[Dict] = []
        self._condition_check_model_classes: List[Type[Model]] = []
        self._delete_items: List[Dict] = []
        self._delete_models: List[Model] = []
        self._put_items: List[Dict] = []
        self._put_models: List[Model] = []
        self._update_items: List[Dict] = []
        self._update_models: List[Model] = []

    def condition_check(
        self,
        model_cls: Type[_M],
        hash_key: _KeyType,
        range_key: Optional[_KeyType] = None,
        condition: Optional[Condition] = None,
        return_values: Optional[str] = None,
    ):
        if condition is None:
            raise TypeError('`condition` cannot be None')
        operation_kwargs = model_cls.get_operation_kwargs_from_class(
            hash_key,
            range_key=range_key,
            condition=condition,
            return_values_on_condition_failure=return_values,
        )
        self._condition_check_items.append(operation_kwargs)
        self._condition_check_model_classes.append(model_cls)

    def delete(
        self,
        model: _M,
        condition: Optional[Condition] = None,
        return_values: Optional[str] = None,
        *,
        add_version_condition: bool = True,
    ) -> None:
        operation_kwargs = model.get_delete_kwargs_from_instance(
            condition=condition,
            return_values_on_condition_failure=return_values,
            add_version_condition=add_version_condition,
        )
        self._delete_items.append(operation_kwargs)
        self._delete_models.append(model)

    def save(self, model: _M, condition: Optional[Condition] = None, return_values: Optional[str] = None) -> None:
        operation_kwargs = model.get_save_kwargs_from_instance(
            condition=condition,
            return_values_on_condition_failure=return_values
        )
        self._put_items.append(operation_kwargs)
        self._put_models.append(model)

    def update(self, model: _M, actions: List[Action], condition: Optional[Condition] = None,
               return_values: Optional[str] = None,
               *,
               add_version_condition: bool = True) -> None:
        operation_kwargs = model.get_update_kwargs_from_instance(
            actions=actions,
            condition=condition,
            return_values_on_condition_failure=return_values,
            add_version_condition=add_version_condition,
        )
        self._update_items.append(operation_kwargs)
        self._update_models.append(model)

    def _commit(self) -> Any:
        try:
            response = self._connection.transact_write_items(
                condition_check_items=self._condition_check_items,
                delete_items=self._delete_items,
                put_items=self._put_items,
                update_items=self._update_items,
                client_request_token=self._client_request_token,
                return_consumed_capacity=self._return_consumed_capacity,
                return_item_collection_metrics=self._return_item_collection_metrics,
            )
        except TransactWriteError as error:
            self._add_items_to_cancellation_reasons(error)
            raise

        for model in [*self._put_models, *self._update_models]:
            model.update_local_version_attribute()
        return response

    def _add_items_to_cancellation_reasons(self, error: TransactWriteError) -> None:
        model_classes: List[Type[Model]] = [
            *self._condition_check_model_classes,
            *(type(model) for model in self._delete_models),
            *(type(model) for model in self._put_models),
            *(type(model) for model in self._update_models),
        ]
        for index, reason in enumerate(error.cancellation_reasons):
            if reason is None or reason.raw_item is None or index >= len(model_classes):
                continue
            try:
                reason.item = model_classes[index].from_raw_data(reason.raw_item)
            except Exception:
                log.exception("Unable to deserialize item returned after transaction cancellation")
