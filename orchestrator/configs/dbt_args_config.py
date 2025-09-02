import json
from typing import Any, Dict, Optional
from dagster import Config
from pydantic import Field, field_validator

class DbtArgsConfig(Config):
    write_command: str = Field(
        default="build",
        description="The primary dbt write command to execute. Options: 'build' or 'run'."
    )
    test_after_run: Optional[bool] = Field(
        default=False,
        description="Whether to run dbt tests after the run command."
    )
    args: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description=(
            "Additional CLI arguments for dbt. For passing variables, supply them as a dict under the 'vars' key; "
            "they will be automatically converted to a JSON string."
        )
    )

    @field_validator("args", mode="before")
    @classmethod
    def convert_vars(cls, v: Any) -> Dict[str, Any]:
        if not isinstance(v, dict):
            raise ValueError("args must be a dictionary")
        if "vars" in v and isinstance(v["vars"], dict):
            v["vars"] = json.dumps(v["vars"])
        return v