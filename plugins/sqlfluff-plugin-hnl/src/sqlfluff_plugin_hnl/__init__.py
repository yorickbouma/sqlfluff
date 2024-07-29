"""An sqlfluff plugin with HNL custom rules.

This uses the rules API supported from 0.4.0 onwards.
"""

from typing import List, Type

from sqlfluff.core.config import ConfigLoader
from sqlfluff.core.plugin import hookimpl
from sqlfluff.core.rules import BaseRule

@hookimpl
def get_rules() -> List[Type[BaseRule]]:
    """Get plugin rules."""

    from sqlfluff_plugin_hnl.rules import Rule_HNL_A001  # noqa: F811

    return [Rule_HNL_A001]


@hookimpl
def load_default_config() -> dict:
    """Loads the default configuration for the plugin."""
    return ConfigLoader.get_global().load_config_resource(
        package="sqlfluff_plugin_hnl",
        file_name="plugin_default_config.cfg",
    )


@hookimpl
def get_configs_info() -> dict:
    """Get rule config validations and descriptions."""
    return {
        "forbidden_columns": {"definition": "A list of column to forbid"},
    }
