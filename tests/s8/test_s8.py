from fastapi.testclient import TestClient


class Tests:

    def test_aircraft_endpoint_responds(self, client: TestClient) -> None:
        response = client.get("/api/s8/aircraft")
        assert response.status_code == 200

    def test_aircraft_pagination_responds(self, client: TestClient) -> None:
        response = client.get("/api/s8/aircraft?num_results=10&page=1")
        assert response.status_code == 200

    def test_co2_endpoint_responds(self, client: TestClient) -> None:
        icao = "a835af"
        response = client.get(f"/api/s8/aircraft/{icao}/co2?hours_flown=10")
        assert response.status_code == 200







