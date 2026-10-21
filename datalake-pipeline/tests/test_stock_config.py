from src.stock_pipeline import config


def test_alpha_vantage_endpoint_datasets_are_exposed_for_dataset_usage():
    endpoint_datasets = [endpoint["dataset"] for endpoint in config.ALPHA_VANTAGE_ENDPOINTS]

    assert endpoint_datasets == [
        "daily_time_series",
        "company_overview",
    ]


def test_get_dataset_name_by_function_resolves_function_to_dataset():
    dataset_by_function = config.get_dataset_name_by_function("TIME_SERIES_DAILY")
    overview_by_function = config.get_dataset_name_by_function("OVERVIEW")

    assert dataset_by_function == "daily_time_series"
    assert overview_by_function == "company_overview"


def test_get_dataset_name_by_function_raises_for_unknown_function():
    import pytest

    with pytest.raises(ValueError):
        config.get_dataset_name_by_function("UNKNOWN_FUNCTION")
