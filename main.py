import aiofiles
import boto3
import pika
import redis
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI()

# RabbitMQ config
RABBITMQ_USER = "uhpixgmt"
RABBITMQ_PASS = "ZSmqBbrwwaF8ZK0LlCBUdxlNLdYWNA8Z"
RABBITMQ_HOST = "crow.rmq.cloudamqp.com"
RABBITMQ_PORT = 5672
RABBITMQ_VHOST = "uhpixgmt"
RABBITMQ_QUEUE = "song_requests"

# AWS S3 config
AWS_ACCESS_KEY = "2bur9lrdin12381u"
AWS_SECRET_KEY = "a19ff9b8-2249-48df-92b0-bb49595d6922"
AWS_REGION = "us-east-1"
AWS_BUCKET_NAME = "seyed-amirhosein-salahi"

# Redis config
REDIS_HOST = "tai.liara.cloud"
REDIS_PORT = 32414
REDIS_PASSWORD = "AnvCzNQOujygvIBcjAjY8KZz"

# Connect to Redis
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)

# Connect to RabbitMQ
rabbitmq_params = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    port=RABBITMQ_PORT,
    virtual_host=RABBITMQ_VHOST,
    credentials=pika.credentials.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
)
connection = pika.BlockingConnection(rabbitmq_params)
channel = connection.channel()
channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)


@app.post("/request/")
async def create_request(email: str, file: UploadFile = File(...)):
    # Save the file to cloud storage
    async with aiofiles.open(file.filename, "wb") as out_file:
        content = await file.read()
        await out_file.write(content)

    # Connect to object storage (compatible with S3)
    s3_client = boto3.client('s3',
                             endpoint_url='https://storage.iran.liara.space',
                             aws_access_key_id=AWS_ACCESS_KEY,
                             aws_secret_access_key=AWS_SECRET_KEY)

    # Upload the file to S3 bucket
    try:
        s3_client.upload_file(file.filename, AWS_BUCKET_NAME, file.filename)
    except Exception as e:
        raise HTTPException(status_code=500, detail=e.args)

    # Save request info to Redis
    request_id = redis_client.incr("request_id")
    redis_key = f"request:{request_id}"
    redis_client.hset(redis_key, "email", email)
    redis_client.hset(redis_key, "status", "pending")
    redis_client.hset(redis_key, "song_id", "")

    # Send request ID to RabbitMQ for processing
    properties = pika.BasicProperties(headers={'request_id': str(request_id)})
    channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE, properties=properties,
                          body=file.filename.encode('utf-8'))

    # Return success response
    return JSONResponse(content={"message": "Request received successfully", "request_id": request_id})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
