# `aiochclient._types.PY_TYPES_MAPPING` is not accessible from outside, so we
# can't use it.
from aiochclient.types import PY_TYPES_MAPPING


def escape(value):
    value_type = type(value)
    try:
        value = PY_TYPES_MAPPING[value_type](value)
    except KeyError:
        # Fallback to slower way.  And even this won't work for virtual
        # subclasses.
        for subclass in value_type.mro()[1:]:
            if subclass in PY_TYPES_MAPPING:
                value = PY_TYPES_MAPPING[subclass](value)
                break
        else:
            raise TypeError(f'Unsupported type {value_type}')
    return value.decode('utf-8')
