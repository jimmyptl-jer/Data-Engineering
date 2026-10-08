from src.stock_pipeline import config


def test_alpha_vantage_endpoint_paths_are_exposed_for_dataset_usage():
    endpoint_paths = [endpoint["path"] for endpoint in config.ALPHA_VANTAGE_ENDPOINTS]

    assert endpoint_paths == [
        "daily_time_series",
        "weekly_time_series",
        "company_overview",
    ]


def test_get_alpha_vantage_endpoint_resolves_by_path_and_function():
    endpoint_by_path = config.get_alpha_vantage_endpoint(path="company_overview")
    endpoint_by_function = config.get_alpha_vantage_endpoint(function="TIME_SERIES_DAILY")

    assert endpoint_by_path["function"] == "OVERVIEW"
    assert endpoint_by_function["path"] == "daily_time_series"
