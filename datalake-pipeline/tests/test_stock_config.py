from src.stock_pipeline import config


def test_alpha_vantage_endpoint_datasets_are_exposed_for_dataset_usage():
    """
    Verify that the Alpha Vantage endpoint registry exposes the expected
    dataset names — these drive the Bronze/Silver ``dataset=`` partitions.
    """
    endpoint_datasets = [endpoint["dataset"] for endpoint in config.ALPHA_VANTAGE_ENDPOINTS]

    assert endpoint_datasets == [
        "daily_time_series",
        "company_overview",
    ]


def test_get_dataset_name_by_function_resolves_function_to_dataset():
    """
    Verify that ``get_dataset_name_by_function()`` resolves API function
    names (e.g. ``TIME_SERIES_DAILY``) to their Silver dataset identifiers.
    """
    dataset_by_function = config.get_dataset_name_by_function("TIME_SERIES_DAILY")
    overview_by_function = config.get_dataset_name_by_function("OVERVIEW")

    assert dataset_by_function == "daily_time_series"
    assert overview_by_function == "company_overview"


def test_get_dataset_name_by_function_raises_for_unknown_function():
    """
    Verify that ``get_dataset_name_by_function()`` raises ``ValueError``
    for an unknown API function name.
    """
    import pytest

    with pytest.raises(ValueError):
        config.get_dataset_name_by_function("UNKNOWN_FUNCTION")
