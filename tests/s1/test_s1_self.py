import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient


class TestManualCoverage:
    """
    Test to check the overall test coverage percentage by comparing
    defined endpoints vs. tested endpoints.
    Ensures that at least 80% of the API endpoints are covered by tests.
    """

    def test_api_coverage(self, client: TestClient, app) -> None:
        # Get all defined endpoints in FastAPI app for s1
        all_routes = {
            (route.path.replace("{icao}", "icao").replace("{item_id}", "item_id"), list(route.methods)[0])
            for route in app.router.routes if isinstance(route, APIRoute) and route.path.startswith("/api/s1")
        }

        #List of tested endpoints (normalized to match FastAPI's format)
        tested_routes = {
            ('/api/s1/aircraft/download', 'POST'),
            ('/api/s1/aircraft/prepare', 'POST'),
            ('/api/s1/aircraft/icao/positions', 'GET'),
            ('/api/s1/aircraft/icao/stats', 'GET'),
            ('/api/s1/aircraft/', 'GET'),
        }

        #Calculate coverage percentage
        covered_routes = tested_routes.intersection(all_routes)
        coverage_percentage = (len(covered_routes) / len(all_routes)) * 100

        #Assert at least 80% coverage
        assert coverage_percentage >= 80, f"Test coverage too low: {coverage_percentage:.2f}% (must be at least 80%)"



class TestAPICompleteness:
    """Ensure all required API fields and behaviors are present."""

    @pytest.fixture(scope="class")
    def valid_icao(self, client: TestClient):
        """Fetch a valid ICAO dynamically from the API."""
        response = client.get("/api/s1/aircraft")
        assert response.status_code == 200, "Failed to get aircraft list"
        aircraft_list = response.json()

        assert isinstance(aircraft_list, list), "Response should be a list"
        assert len(aircraft_list) > 0, "No aircraft found in the response"

        return aircraft_list[0]["icao"]  # Return the first available ICAO code

    def test_aircraft_list_completeness(self, client: TestClient) -> None:
        """Check if the /api/s1/aircraft endpoint returns a complete list of aircraft."""
        response = client.get("/api/s1/aircraft")
        assert response.status_code == 200, "Aircraft list endpoint failed"
        aircraft_list = response.json()
        assert isinstance(aircraft_list, list), "Response should be a list"
        assert len(aircraft_list) > 0, "No aircraft data returned"

        required_fields = ["icao", "registration", "type"]
        for field in required_fields:
            assert field in aircraft_list[0], f"Missing field: {field}"

    def test_aircraft_stats_completeness(self, client: TestClient, valid_icao: str) -> None:
        """Check if aircraft stats include all required fields dynamically."""
        response = client.get(f"/api/s1/aircraft/{valid_icao}/stats")
        assert response.status_code == 200, "Aircraft stats endpoint failed"

        stats = response.json()
        required_fields = ["max_altitude_baro", "max_ground_speed", "had_emergency"]
        for field in required_fields:
            assert field in stats, f"Missing field in stats: {field}"

    def test_aircraft_positions_completeness(self, client: TestClient, valid_icao: str) -> None:
        """Check if aircraft position responses contain all expected fields dynamically."""
        response = client.get(f"/api/s1/aircraft/{valid_icao}/positions")
        assert response.status_code == 200, "Aircraft positions endpoint failed"

        positions = response.json()
        assert isinstance(positions, list), "Positions should be a list"
        assert len(positions) > 0, "No position data returned"

        required_fields = ["timestamp", "lat", "lon"]
        for field in required_fields:
            assert field in positions[0], f"Missing field: {field}"





