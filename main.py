import time
from fastapi import FastAPI, Request, HTTPException, Depends

app = FastAPI()

# In-memory storage for IP addresses and their tokens
ip_buckets = {}

# 1. Define the Rate Limiter Logic
class RateLimiter:
    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate

    def __call__(self, request: Request):
        client_ip = request.client.host if request.client else "127.0.0.1"
        current_time = time.time()
        # Initialize bucket for a new IP
        if client_ip not in ip_buckets:
            ip_buckets[client_ip] = {
                "tokens": self.capacity,
                "last_updated": current_time,
            }

        bucket = ip_buckets[client_ip]
        
        # Calculate how many tokens to add based on time passed
        elapsed_time = current_time - bucket["last_updated"]
        bucket["tokens"] = min(
            self.capacity, 
            bucket["tokens"] + (elapsed_time * self.refill_rate)
        )
        bucket["last_updated"] = current_time

        # Allow or block the request
        if bucket["tokens"] >= 1.0:
            bucket["tokens"] -= 1.0
            print(client_ip, current_time, bucket["tokens"], bucket["last_updated"])
            return True
        else:
            print(client_ip, current_time, bucket["tokens"], bucket["last_updated"])
            raise HTTPException(
                status_code=429, 
                detail="Rate limit exceeded for this specific function."
            )

# 2. Inject the Rate Limiter into your endpoint
@app.get("/", dependencies=[Depends(RateLimiter(capacity=3, refill_rate=0.5))])
def test():
    return {"message": "hello"}

# 1..5 | ForEach-Object { curl.exe -i http://127.0.0.1:8000/ }