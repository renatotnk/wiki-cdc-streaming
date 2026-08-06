"""Reads a contracts/*.contract.yaml file's top-level field names -- the P8
fail-fast check (SPEC-agnostic-architecture.md Section 4.4): a contracted
field missing from what a layer actually produces should fail loud, at
pipeline-registration time, not silently propagate. Whether a field is
nullable governs its *values*, not whether the *column* must exist -- every
listed field, nullable or not, is part of the contract.

Not an SDP `@dp.expect_or_fail`: that decorator doesn't exist in open-source
Spark (pipelines/silver/dq_rules.py's module docstring). A plain assertion
at module-import time achieves the same fail-fast intent -- `spark-pipelines`
imports every pipeline file as a module, so a raised exception here crashes
registration exactly as loud as bronze's typing-based fail-fast already does
(SPEC-phase2-bronze.md Section 4).

Only top-level field names are read -- nested struct fields (e.g. bronze's
meta/length) aren't independently checked here; the struct field itself
(e.g. "meta") being present is what's being verified.
"""

from pathlib import Path

import yaml


def required_field_names(contract_path: Path) -> set[str]:
    contract = yaml.safe_load(contract_path.read_text())
    return {field["name"] for field in contract["fields"]}
