import pytest

from src.stock_pipeline import config


def test_alpha_vantage_endpoint_datasets_are_exposed_for_dataset_usage():
    datasets = [endpoint["dataset"] for endpoint in config.ALPHA_VANTAGE_ENDPOINTS]

    assert datasets == [
        "daily_time_series",
        "company_overview",
    ]


def test_get_dataset_name_by_function_resolves_registered_functions():
    assert config.get_dataset_name_by_function("TIME_SERIES_DAILY") == "daily_time_series"
    assert config.get_dataset_name_by_function("OVERVIEW") == "company_overview"


def test_get_dataset_name_by_function_rejects_unknown_function():
    with pytest.raises(ValueError):
        config.get_dataset_name_by_function("TIME_SERIES_WEEKLY")


def test_every_watermarked_dataset_is_a_registered_endpoint():
    datasets = {endpoint["dataset"] for endpoint in config.ALPHA_VANTAGE_ENDPOINTS}

    assert set(config.WATERMARK_STRATEGIES) <= datasets
