from functools import wraps
from typing import Any, Callable, Optional

from pydantic import BaseModel


def wrap_tool_call(name: str, description: str, schema: Optional[BaseModel] = None):
    """
    Decorator to wrap a python function as an OAM-compatible tool.
    Injects metadata that the OAM agent can inspect.

    Args:
        name: The name of the tool exposed to the LLM
        description: A description of what the tool does
        schema: Optional Pydantic model defining the input schema
    """

    def decorator(func: Callable) -> Callable:
        # Attach metadata to the function object itself
        tool_def: dict[str, Any] = {
            "name": name,
            "description": description,
            "python_function": func.__name__,
        }

        if schema:
            tool_def["input_schema"] = schema.model_json_schema()

        setattr(func, "oam_tool_def", tool_def)

        @wraps(func)
        def wrapper(*args, **kwargs):
            # This wrapper could also intercept calls for logging/metrics
            return func(*args, **kwargs)

        # Ensure metadata is preserved on the wrapper
        setattr(wrapper, "oam_tool_def", tool_def)
        return wrapper

    return decorator
