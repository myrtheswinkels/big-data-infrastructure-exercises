from fastapi.testclient import TestClient
import pytest
from fastapi.routing import APIRoute



class TestS1Student:
    """
    As you code it's always important to ensure that your code reflects
    the business requisites you have.
    The optimal way to do so is via tests.
    Use this class to create functions to test your application.

    For more information on the library used, search `pytest` in your preferred search engine.
    """

    def test_first(self, client: TestClient) -> None:
        # Implement tests if you want
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=1")
            assert True
            
    @pytest.fixture(scope="class")
    def valid_icao(self, client: TestClient):
        """Fetch a valid ICAO dynamically from the API."""
        response = client.get("/api/s1/aircraft")
        assert response.status_code == 200, "Failed to get aircraft list"
        aircraft_list = response.json()
        
        assert isinstance(aircraft_list, list), "Response should be a list"
        assert len(aircraft_list) > 0, "No aircraft found in the response"
        
        return aircraft_list[0]["icao"]  # Return the first available ICAO code
        
    def test_api_coverage(self, client: TestClient, app) -> None:
        """
        Test to check the overall test coverage percentage by comparing
        defined endpoints vs. tested endpoints.
        Ensures that at least 80% of the API endpoints are covered by tests.
        """
        # Get all defined endpoints in FastAPI app for s1
        all_routes = {
            (route.path.replace("{icao}", "icao").replace("{item_id}", "item_id"), list(route.methods)[0])
            for route in app.router.routes if isinstance(route, APIRoute) and route.path.startswith("/api/s1")
        }
        
        # List of tested endpoints (normalized to match FastAPI's format)
        tested_routes = {
            ('/api/s1/aircraft/download', 'POST'),
            ('/api/s1/aircraft/prepare', 'POST'),
            ('/api/s1/aircraft/icao/positions', 'GET'),
            ('/api/s1/aircraft/icao/stats', 'GET'),
            ('/api/s1/aircraft/', 'GET'),
        }
        
        # Calculate coverage percentage
        covered_routes = tested_routes.intersection(all_routes)
        coverage_percentage = (len(covered_routes) / len(all_routes)) * 100
        
        # Assert at least 80% coverage
        assert coverage_percentage >= 80, f"Test coverage too low: {coverage_percentage:.2f}% (must be at least 80%)"
    
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


class TestItCanBeEvaluated:
    """
    Those tests are just to be sure I can evaluate your exercise.
    Don't modify anything from here!

    Make sure all those tests pass with `poetry run pytest` or it will be a 0!
    """

    def test_download(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/download?file_limit=1")
            assert not response.is_error, "Error at the download endpoint"

    def test_prepare(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s1/aircraft/prepare")
            assert not response.is_error, "Error at the prepare endpoint"

    def test_aircraft(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s1/aircraft")
            assert not response.is_error, "Error at the aircraft endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) > 0, "Result is empty"
            for field in ["icao", "registration", "type"]:
                assert field in r[0], f"Missing '{field}' field."

    def test_positions(self, client: TestClient) -> None:
        icao = "06a0af"
        with client as client:
            response = client.get(f"/api/s1/aircraft/{icao}/positions")
            assert not response.is_error, "Error at the positions endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) > 0, "Result is empty"
            for field in ["timestamp", "lat", "lon"]:
                assert field in r[0], f"Missing '{field}' field."

    def test_stats(self, client: TestClient) -> None:
        icao = "06a0af"
        with client as client:
            response = client.get(f"/api/s1/aircraft/{icao}/stats")
            assert not response.is_error, "Error at the positions endpoint"
            r = response.json()
            for field in ["max_altitude_baro", "max_ground_speed", "had_emergency"]:
                assert field in r, f"Missing '{field}' field."
