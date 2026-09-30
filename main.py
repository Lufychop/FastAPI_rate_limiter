import time
from contextlib import asynccontextmanager
import redis.asyncio as aioredis
from fastapi import FastAPI, Request, HTTPException, Depends

# 1. Manage Redis client lifecycle cleanly
redis_client: aioredis.Redis = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = aioredis.from_url(
        "redis://localhost:6379", 
        decode_responses=True
    )
    yield
    await redis_client.close()

app = FastAPI(lifespan=lifespan)

# Atomic Lua script for Token Bucket evaluation inside Redis
TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local ttl = math.ceil(capacity / refill_rate)

-- Fetch current bucket state from Redis hash
local data = redis.call('HMGET', key, 'tokens', 'last_updated')
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if not tokens then
    -- First time seeing this IP: start with full capacity
    tokens = capacity
    last_updated = now
else
    -- Compute refill math based on time elapsed
    local elapsed = now - last_updated
    tokens = math.min(capacity, tokens + (elapsed * refill_rate))
    last_updated = now
end

-- Check if enough tokens exist
if tokens >= cost then
    tokens = tokens - cost
    redis.call('HSET', key, 'tokens', tokens, 'last_updated', last_updated)
    redis.call('EXPIRE', key, ttl)  -- Auto-delete key if inactive for full refill period
    return {1, tostring(tokens)}   -- Allowed (1)
else
    redis.call('HSET', key, 'tokens', tokens, 'last_updated', last_updated)
    redis.call('EXPIRE', key, ttl)
    return {0, tostring(tokens)}   -- Blocked (0)
end
"""

class RedisRateLimiter:
    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate

    async def __call__(self, request: Request):
        client_ip = request.client.host if request.client else "127.0.0.1"
        key = f"rate_limit:{client_ip}"
        now = time.time()

        try:
            allowed, remaining_tokens = await redis_client.eval(
                TOKEN_BUCKET_LUA,
                1,  # Number of keys
                key,
                self.capacity,
                self.refill_rate,
                1.0,  # Token cost per request
                now,
            )
        except aioredis.RedisError as e:
            # Fallback strategy: Fail open if Redis is down so the API stays active
            print(f"Warning: Redis rate limiter unavailable ({e}). Allowing request.")
            return True

        if allowed == 1:
            return True
        else:
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded.",
                headers={"Retry-After": "2"},
            )

# Dependency Injection usage
@app.get("/items/", dependencies=[Depends(RedisRateLimiter(capacity=3, refill_rate=0.5))])
async def read_items():
    return [{"item_name": "Foo"}, {"item_name": "Bar"}]