"""Options of a decorated per-job or per-record function, checked when the step is built.

``@transformation``, ``@record_transformation`` and ``@record_destination`` turn a
function ``(records_or_record, runtime_params, **options)`` into a class whose
constructor takes the options. They are bound against the function's signature right
there, so a typo or a missing option fails where the step is built
(``define_transformations`` / ``define_destination``), not mid-job.
"""

import inspect
from typing import Any, Callable, Dict


def option_binder(func: Callable[..., Any], owner: str) -> Callable[[Dict[str, Any]], None]:
    """A checker raising ``TypeError`` (naming ``owner``) for options ``func`` does not take.

    The signature is read once, here; two placeholders stand for the two leading
    positional parameters.
    """
    signature = inspect.signature(func)

    def bind(options: Dict[str, Any]) -> None:
        try:
            signature.bind(None, None, **options)
        except TypeError as exc:
            raise TypeError(f"{owner} ({func.__name__}): {exc}") from None

    return bind
