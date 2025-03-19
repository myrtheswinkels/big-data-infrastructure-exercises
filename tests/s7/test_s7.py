import pytest
from fastapi.testclient import TestClient
from fastapi.routing import APIRoute
import flake8.main.application

class Tests:

    def test_prepare(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s7/aircraft/prepare")
            assert not response.is_error, "Error at the prepare endpoint"

    def test_aircraft(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s7/aircraft")
            assert not response.is_error, "Error at the aircraft endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) > 0, "Result is empty"
            for field in ["icao", "registration", "type"]:
                assert field in r[0], f"Missing '{field}' field."

    def test_positions(self, client: TestClient) -> None:
        icao = "06a0af"
        with client as client:
            response = client.get(f"/api/s7/aircraft/{icao}/positions")
            assert not response.is_error, "Error at the positions endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) > 0, "Result is empty"
            for field in ["timestamp", "lat", "lon"]:
                assert field in r[0], f"Missing '{field}' field."

    def test_stats(self, client: TestClient) -> None:
        icao = "06a0af"
        with client as client:
            response = client.get(f"/api/s7/aircraft/{icao}/stats")
            assert not response.is_error, "Error at the positions endpoint"
            r = response.json()
            for field in ["max_altitude_baro", "max_ground_speed", "had_emergency"]:
                assert field in r, f"Missing '{field}' field."


    def valid_icao(self, client: TestClient):
        """Fetch a valid ICAO dynamically from the API."""
        response = client.get("/api/s7/aircraft")
        assert response.status_code == 200, "Failed to get aircraft list"
        aircraft_list = response.json()
        
        assert isinstance(aircraft_list, list), "Response should be a list"
        assert len(aircraft_list) > 0, "No aircraft found in the response"
        
        return aircraft_list[0]["icao"]  # Return the first available ICAO code
    
    def test_api_coverage(self, client: TestClient, app) -> None:   
        # Get all defined endpoints in FastAPI app for s7
        all_routes = {
            (route.path.replace("{icao}", "icao").replace("{item_id}", "item_id"), list(route.methods)[0])
            for route in app.router.routes if isinstance(route, APIRoute) and route.path.startswith("/api/s7")
        }
        
        # List of tested endpoints (normalized to match FastAPI's format)
        tested_routes = {
            ('/api/s7/aircraft/prepare', 'POST'),
            ('/api/s7/aircraft/icao/positions', 'GET'),
            ('/api/s7/aircraft/icao/stats', 'GET'),
            ('/api/s7/aircraft/', 'GET'),
        }
        
        # Calculate coverage percentage
        covered_routes = tested_routes.intersection(all_routes)
        coverage_percentage = (len(covered_routes) / len(all_routes)) * 100
        
        # Assert at least 80% coverage
        assert coverage_percentage >= 80, f"API test coverage is {coverage_percentage}%"

       
    def test_aircraft_list_completeness(self, client: TestClient) -> None:
        """Check if the /api/s7/aircraft endpoint returns a complete list of aircraft."""
        response = client.get("/api/s7/aircraft")
        assert response.status_code == 200, "Aircraft list endpoint failed"
        aircraft_list = response.json()
        assert isinstance(aircraft_list, list), "Response should be a list"
        assert len(aircraft_list) > 0, "No aircraft data returned"
        
        required_fields = ["icao", "registration", "type"]
        for field in required_fields:
            assert field in aircraft_list[0], f"Missing field: {field}"
