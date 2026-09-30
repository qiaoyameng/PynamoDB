from typing import Dict
from typing import List
from typing import Union

from pynamodb.attributes import Attribute
from pynamodb.expressions.operand import Path
from pynamodb.expressions.util import substitute_names


def create_projection_expression(attributes_to_get, placeholders: Dict[str, str]) -> str:
    expressions = [substitute_names(path, placeholders) for path in get_projection_document_paths(attributes_to_get)]
    return ', '.join(expressions)


def get_projection_document_paths(attributes_to_get) -> List[List[str]]:
    """
    Normalizes a projection (a single attribute or a list of attributes, where each attribute
    is a string document path, an `Attribute`, or a `Path`) into a list of document paths,
    each represented as a list of path segments.
    """
    if isinstance(attributes_to_get, (str, Attribute, Path)):
        attributes_to_get = [attributes_to_get]
    return [_get_document_path(attribute) for attribute in attributes_to_get]


def _get_document_path(attribute: Union[Attribute, Path, str]) -> List[str]:
    if isinstance(attribute, Attribute):
        return [attribute.attr_name]
    if isinstance(attribute, Path):
        return attribute.path
    return attribute.split('.')
