"""DatabricksJobsHandler: builds the right Jobs API requests, with no real
network call -- mocks `requests.post` itself (FIRST: Isolated), same
technique as tests/test_s3_compatible_storage_handler.py. Never touches
DATABRICKS_HOST/DATABRICKS_TOKEN secrets -- this test supplies fake values
directly, confirming the class needs nothing from the environment itself
(the deploy *script* is what reads env vars, not the handler).
"""

from unittest.mock import MagicMock, patch

from src.handlers.deploy.databricks_jobs_handler import DatabricksJobsHandler

REQUESTS_POST_PATH = "src.handlers.deploy.databricks_jobs_handler.requests.post"


def test_update_job_definition_posts_the_notebook_path_to_the_jobs_update_endpoint():
    mock_response = MagicMock()
    handler = DatabricksJobsHandler(host="https://example.cloud.databricks.com", token="fake-token")

    with patch(REQUESTS_POST_PATH, return_value=mock_response) as mock_post:
        handler.update_job_definition(job_id="123", notebook_path="/Repos/wiki-cdc-streaming/pipelines/bronze")

    mock_post.assert_called_once()
    call = mock_post.call_args
    assert call.args[0] == "https://example.cloud.databricks.com/api/2.1/jobs/update"
    assert call.kwargs["headers"] == {"Authorization": "Bearer fake-token"}
    assert call.kwargs["json"]["job_id"] == "123"
    task = call.kwargs["json"]["new_settings"]["tasks"][0]
    assert task["notebook_task"]["notebook_path"] == "/Repos/wiki-cdc-streaming/pipelines/bronze"
    mock_response.raise_for_status.assert_called_once()


def test_trigger_run_posts_to_run_now_and_returns_the_run_id():
    mock_response = MagicMock()
    mock_response.json.return_value = {"run_id": 456}
    handler = DatabricksJobsHandler(host="https://example.cloud.databricks.com", token="fake-token")

    with patch(REQUESTS_POST_PATH, return_value=mock_response) as mock_post:
        run_id = handler.trigger_run(job_id="123")

    mock_post.assert_called_once_with(
        "https://example.cloud.databricks.com/api/2.1/jobs/run-now",
        headers={"Authorization": "Bearer fake-token"},
        json={"job_id": "123"},
        timeout=30,
    )
    mock_response.raise_for_status.assert_called_once()
    assert run_id == "456"


def test_host_trailing_slash_is_stripped():
    mock_response = MagicMock()
    mock_response.json.return_value = {"run_id": 1}
    handler = DatabricksJobsHandler(host="https://example.cloud.databricks.com/", token="fake-token")

    with patch(REQUESTS_POST_PATH, return_value=mock_response) as mock_post:
        handler.trigger_run(job_id="1")

    assert mock_post.call_args.args[0] == "https://example.cloud.databricks.com/api/2.1/jobs/run-now"
