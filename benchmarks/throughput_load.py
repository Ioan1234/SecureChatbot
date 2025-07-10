#queries per second under load
import json
from locust import HttpUser, between, task

class ChatBotUser(HttpUser):
    wait_time = between(1, 3)

    @task(4)
    def he_query(self):
        #HE query
        payload = {"message": "Who has the highest balance?"}
        headers = {"Content-Type": "application/json"}

        with self.client.post(
            "/api/chat",
            data=json.dumps(payload),
            headers=headers,
            catch_response=True
        ) as response:
            if response.status_code != 200:
                response.failure(f"HE query failed with status {response.status_code}")
            else:
                response.success()

    @task(1)
    def non_he_query(self):
        #non-HE query
        payload = {"message": "Show current asset prices"}
        headers = {"Content-Type": "application/json"}

        with self.client.post(
            "/api/chat",
            data=json.dumps(payload),
            headers=headers,
            catch_response=True
        ) as response:
            if response.status_code != 200:
                response.failure(f"Non-HE query failed with status {response.status_code}")
            else:
                response.success()

    @task(2)
    def execute_sql_query(self):
        #SQL query
        payload = {"sql": "SELECT name FROM brokers LIMIT 5"}
        headers = {"Content-Type": "application/json"}

        with self.client.post(
            "/api/execute_sql",
            data=json.dumps(payload),
            headers=headers,
            catch_response=True
        ) as response:
            if response.status_code != 200:
                response.failure(f"SQL query failed with status {response.status_code}")
            else:
                result = response.json()
                if not result.get("success", False):
                    response.failure("SQL query returned error")
                else:
                    response.success()

