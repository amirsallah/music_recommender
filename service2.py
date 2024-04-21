import boto3
import pika
import redis
import requests

# AWS S3 config
AWS_ACCESS_KEY = "2bur9lrdin12381u"
AWS_SECRET_KEY = "a19ff9b8-2249-48df-92b0-bb49595d6922"
AWS_REGION = "us-east-1"
AWS_BUCKET_NAME = "seyed-amirhosein-salahi"

# Redis config
REDIS_HOST = "tai.liara.cloud"
REDIS_PORT = 32414
REDIS_PASSWORD = "AnvCzNQOujygvIBcjAjY8KZz"

# RabbitMQ connection parameters
RABBITMQ_URL = "amqps://uhpixgmt:ZSmqBbrwwaF8ZK0LlCBUdxlNLdYWNA8Z@crow.rmq.cloudamqp.com/uhpixgmt"
RABBITMQ_QUEUE = "song_requests"

# Shazam API parameters
SHAZAM_API_URL = "https://shazam-api-free.p.rapidapi.com/shazam/recognize/"
SHAZAM_API_HEADERS = {
    "X-RapidAPI-Key": "90daada7acmshf985d8bc7b14882p1efb7ajsn5af69d3c274f",
    "X-RapidAPI-Host": "shazam-api-free.p.rapidapi.com"
}

# Spotify API parameters
SPOTIFY_API_URL = "https://spotify23.p.rapidapi.com/search/"
SPOTIFY_API_HEADERS = {
    "X-RapidAPI-Key": "90daada7acmshf985d8bc7b14882p1efb7ajsn5af69d3c274f",
    "X-RapidAPI-Host": "spotify23.p.rapidapi.com"
}

# Connect to Redis
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)

# Connect to RabbitMQ
parameters = pika.URLParameters(RABBITMQ_URL)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)


def callback(ch, method, properties, body):
    print(f"Received message: {body}")

    # Connect to object storage (compatible with S3)
    s3_client = boto3.client('s3',
                             endpoint_url='https://storage.iran.liara.space',
                             aws_access_key_id=AWS_ACCESS_KEY,
                             aws_secret_access_key=AWS_SECRET_KEY)

    # download file from s3 bucket
    try:
        file_path = body.decode('utf-8')
        s3_client.download_file(AWS_BUCKET_NAME, file_path, f"serv2/{file_path}")
    except Exception as e:
        raise e.args

    files = {"upload_file": open(f"serv2/{file_path}", "rb")}

    # Call Shazam API to recognize song
    shazam_response = requests.post(SHAZAM_API_URL, files=files, headers=SHAZAM_API_HEADERS)
    if shazam_response.status_code == 200:
        shazam_data = shazam_response.json()
        song_title = shazam_data['track']['title']

        # Call Spotify API to search for the song
        spotify_querystring = {"q": song_title, "type": "tracks", "offset": "0", "limit": "1",
                               "numberOfTopResults": "1"}
        spotify_response = requests.get(SPOTIFY_API_URL, headers=SPOTIFY_API_HEADERS, params=spotify_querystring)
        if spotify_response.status_code == 200:
            spotify_data = spotify_response.json()
            spotify_id = spotify_data['tracks']['items'][0]['data']['id']

            # Update request status in the database
            request_id = properties.correlation_id
            request_key = f"request:{request_id}"
            redis_client.hset(request_key, "status", "ready")

            # Update request with SpotifyID in the database
            redis_client.hset(request_key, "spotify_id", spotify_id)

            print(f"SpotifyID for song '{song_title}': {spotify_id}")
        else:
            print("Failed to search for song in Spotify API")
    else:
        print(shazam_response.content)
        print("Failed to recognize song in Shazam API")


# Consume messages from RabbitMQ
channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback, auto_ack=True)

print("Waiting for messages...")
channel.start_consuming()
