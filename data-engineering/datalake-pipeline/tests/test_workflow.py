"""
Tests for the end-to-end Step Functions workflow in template.yaml.

The transform job only reads the Bronze `run_id=` partition it is given, so
every Lambda the workflow calls must receive the same generated run_id, and
nothing else may trigger the ingestion Lambdas with a different one.
"""

import re

import pytest

from conftest import INGESTION_ROOT, PROJECT_ROOT, load_template

INGESTION_FUNCTIONS = {
    "AlphaVantageIngestFunctionName": "alpha_vantage",
    "MassiveIngestFunctionName": "massive",
    "FinnhubIngestFunctionName": "finnhub",
}
TRANSFORM_FUNCTION = "StockPipelineFunction"


@pytest.fixture(scope="module")
def template():
    return load_template(PROJECT_ROOT / "template.yaml")


@pytest.fixture(scope="module")
def definition(template):
    return template["Resources"]["StockPipelineStateMachine"]["Properties"]["Definition"]


def iter_states(states):
    """Yield (name, state) for every state, including those inside Parallel branches."""
    for name, state in states.items():
        yield name, state
        for branch in state.get("Branches", []):
            yield from iter_states(branch["States"])


def lambda_tasks(definition):
    return {
        state["Parameters"]["FunctionName"]["Ref"]: (name, state)
        for name, state in iter_states(definition["States"])
        if state.get("Resource") == "arn:aws:states:::lambda:invoke"
    }


def test_workflow_invokes_every_ingestion_lambda_and_the_transform(definition):
    assert set(lambda_tasks(definition)) == {*INGESTION_FUNCTIONS, TRANSFORM_FUNCTION}


def test_every_lambda_receives_the_generated_run_id(definition):
    create = definition["States"][definition["StartAt"]]
    assert create["Type"] == "Pass"
    assert "run_id.$" in create["Parameters"]

    for function, (name, state) in lambda_tasks(definition).items():
        assert state["Parameters"]["Payload"].get("run_id.$") == "$.run_id", name


def test_transform_runs_after_all_ingestion(definition):
    ingest = definition["States"]["IngestToBronze"]

    assert ingest["Type"] == "Parallel"
    assert ingest["ResultPath"] is None  # keep $.run_id for the transform
    assert len(ingest["Branches"]) == len(INGESTION_FUNCTIONS)
    next_state = definition["States"][ingest["Next"]]
    assert next_state["Parameters"]["FunctionName"] == {"Ref": TRANSFORM_FUNCTION}


@pytest.mark.parametrize("function", INGESTION_FUNCTIONS)
def test_ingestion_failure_status_fails_the_workflow(definition, function):
    states = dict(iter_states(definition["States"]))
    name, task = lambda_tasks(definition)[function]

    choice = states[task["Next"]]
    assert choice["Type"] == "Choice"
    [rule] = choice["Choices"]
    assert rule["Variable"] == "$.result.statusCode"
    assert rule["NumericEquals"] == 500
    assert states[rule["Next"]]["Type"] == "Fail"


@pytest.mark.parametrize("function, source", INGESTION_FUNCTIONS.items())
def test_payload_keys_are_read_by_the_target_lambda(definition, function, source):
    handler_source = (INGESTION_ROOT / source / "app.py").read_text(encoding="utf-8")
    name, task = lambda_tasks(definition)[function]

    for key in task["Parameters"]["Payload"]:
        key = key.removesuffix(".$")
        assert re.search(rf'event\.get\(\s*"{key}"', handler_source), (
            f"{name} sends '{key}' but ingestion/{source}/app.py never reads it"
        )


def test_workflow_is_the_only_scheduled_trigger(template):
    transform = template["Resources"][TRANSFORM_FUNCTION]["Properties"]
    assert "Events" not in transform

    for source in INGESTION_FUNCTIONS.values():
        samconfig = (INGESTION_ROOT / source / "samconfig.toml").read_text(encoding="utf-8")
        assert 'EnableSchedule=\\"false\\"' in samconfig, source
