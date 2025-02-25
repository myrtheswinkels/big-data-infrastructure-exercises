import pytest
from fastapi.testclient import TestClient
from fastapi.routing import APIRoute
import flake8.main.application

class TestItCanBeEvaluated:
    """
    These tests ensure that the exercise can be evaluated.
    
    Run tests using: `poetry run pytest`
    """

    @pytest.fixture
    def test_download(self, client: TestClient) -> None:
        """Test that the download endpoint works without errors."""
        response = client.post("/api/s1/aircraft/download?file_limit=1")
        assert response.status_code == 200, "Error at the download endpoint"
        assert "Files downloaded and uploaded" in response.text, "Unexpected response from download"

    def test_prepare(self, client: TestClient) -> None:
        """Test that the prepare endpoint works without errors."""
        response = client.post("/api/s1/aircraft/prepare")
        assert response.status_code == 200, "Error at the prepare endpoint"
        assert response.text == '"OK"', "Unexpected response from prepare"

    def test_code_cleanliness(self) -> None:
        """Check PEP8 compliance using Flake8."""
        app = flake8.main.application.Application()
        app.run(["bdi_api/s4", "--ignore=E501,W503,E203,F401,F841,E303,W291,W293"])
        assert app.result_count == 0, f"Flake8 found {app.result_count} issues!"

    def test_api_coverage(self, client: TestClient, app) -> None:
        # Get all defined endpoints in FastAPI app for s4
        all_routes = {
            (route.path.replace("{icao}", "icao").replace("{item_id}", "item_id"), list(route.methods)[0])
            for route in app.router.routes if isinstance(route, APIRoute) and route.path.startswith("/api/s4")
        }

        #List of tested endpoints (normalized to match FastAPI's format)
        tested_routes = {
            ('/api/s4/aircraft/download', 'POST'),
            ('/api/s4/aircraft/prepare', 'POST'),
        }

        #Calculate coverage percentage
        covered_routes = tested_routes.intersection(all_routes)
        coverage_percentage = (len(covered_routes) / len(all_routes)) * 100

        #Assert at least 80% coverage
        assert coverage_percentage >= 80, f"Test coverage too low: {coverage_percentage:.2f}% (must be at least 80%)"



