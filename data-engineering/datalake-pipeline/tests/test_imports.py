"""Smoke tests: every entry point imports and every SAM Handler resolves."""

import importlib

import pytest

from conftest import INGESTION_ROOT, load_lambda, load_template

SOURCES = ["alpha_vantage", "finnhub", "massive", "twelvedata"]


@pytest.mark.parametrize("module", [
    "src.stock_pipeline.app",
    "src.stock_pipeline.pipeline",
    "src.stock_pipeline.extract",
    "src.stock_pipeline.transform",
    "src.stock_pipeline.watermark.manager",
])
def test_transform_job_modules_import(module):
    importlib.import_module(module)


@pytest.mark.parametrize("source", SOURCES)
def test_ingestion_stack_handler_resolves(source):
    stack_dir = INGESTION_ROOT / source
    template = load_template(stack_dir / "template.yaml")
    [function] = [
        resource["Properties"]
        for resource in template["Resources"].values()
        if resource["Type"] == "AWS::Serverless::Function"
    ]

    # Each Lambda folder is its own code root, with its own requirements.txt
    code_root = (stack_dir / function["CodeUri"]).resolve()
    assert code_root == stack_dir.resolve()
    assert (code_root / "requirements.txt").is_file()

    assert function["Handler"] == "app.lambda_handler"
    assert callable(load_lambda(source).app.lambda_handler)
