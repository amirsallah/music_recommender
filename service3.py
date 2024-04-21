import redis
import requests
import time

# Redis config
REDIS_HOST = "tai.liara.cloud"
REDIS_PORT = 32414
REDIS_PASSWORD = "AnvCzNQOujygvIBcjAjY8KZz"

# Spotify API parameters
SPOTIFY_API_URL = "https://spotify23.p.rapidapi.com/recommendations/"
SPOTIFY_API_HEADERS = {
    "X-RapidAPI-Key": "90daada7acmshf985d8bc7b14882p1efb7ajsn5af69d3c274f",
    "X-RapidAPI-Host": "spotify23.p.rapidapi.com"
}

# Connect to Redis
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)

# Define the interval for checking requests in seconds
CHECK_INTERVAL = 5  # Check every 60 seconds


# Function to process requests
def process_requests():
    # Read requests with status "ready" from Redis
    print("i am trying to read")
    ready_requests = redis_client.hgetall("request:*")

    for request_id, song_id in ready_requests.items():
        # Send request to Spotify API
        querystring = {"limit": "3", "seed_tracks": song_id}
        spotify_response = requests.get(SPOTIFY_API_URL, headers=SPOTIFY_API_HEADERS, params=querystring)

        if spotify_response.status_code == 200:
            spotify_data = spotify_response.json()
            # Process recommendations...

            # Update request status to "done" in Redis
            redis_client.hset(f"request:{request_id}", "status", "done")
        else:
            print("Failed to get recommendations from Spotify API")


# Main loop
while True:
    process_requests()
    time.sleep(CHECK_INTERVAL)
