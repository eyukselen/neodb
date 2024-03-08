from locust import FastHttpUser, task, between, events


class CreateBuckets(FastHttpUser):
    wait_time = between(0, 0.5)  # Adjust wait time between requests as needed
    bucket_count = 0
    user_id = 0

    @task
    def create_buckets(self):
        # Replace with your specific data and URL
        self.user_id += 1
        user_name = "user_" + str(self.user_id)
        # how to break the test
        if self.user_id > 10:
            self.environment.runner.quit()

        bucket_url = "/bucket-" + "user_" + str(self.user_id)
        response = self.client.post(f"/buckets{bucket_url}")
        response.raise_for_status()  # Raise an error if request fails
        for x in range(10):
            response = self.client.post(f"/buckets{bucket_url}/bucket-{x}", name=f"/buckets{bucket_url}")


if __name__ == "__main__":
    from locust import User

    User(CreateBuckets, count=10, spawn_rate=10)
    User()
