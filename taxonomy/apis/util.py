import time


class RateLimiter:
    def __init__(self, min_interval: float) -> None:
        self.min_interval = min_interval
        self.last_time = 0.0

    def wait(self) -> None:
        elapsed = time.time() - self.last_time
        wait_time = self.min_interval - elapsed
        if wait_time > 0:
            time.sleep(wait_time)
        self.last_time = time.time()
