#queries per second under loadfrom locust import HttpUser, task, between
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

