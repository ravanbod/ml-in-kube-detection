import pika
import os
from dotenv import load_dotenv
import json
from ultralytics import YOLO
import requests
from minio import Minio
import datetime

def process_image(ch, method, properties, body):
    data = json.loads(body)
    image_id, image_url = data["id"], data["url"]
    # Download and save image
    image_content = requests.get(image_url).content
    saved_file_path = "/tmp/" + image_id + "." + "jpg"
    f = open(saved_file_path, "wb")
    f.write(image_content)
    f.close()

    # Predict image
    results = ml_model(saved_file_path, save=True, project="/tmp")
    predicted_file_path = results[0].save_dir + "/" + saved_file_path.split("/")[-1]
    print(predicted_file_path)

    # Upload image to minio
    print("uploading to minio")
    client = Minio(
        os.environ["S3_URL"],
        access_key=os.environ["S3_ACCESS_KEY"],
        secret_key=os.environ["S3_SECRET_KEY"],
        secure=False,
    )
    client.fput_object("bucket1", image_id + "." + "jpg", predicted_file_path)
    predicted_file_url = client.presigned_get_object(
        "bucket1", image_id + "." + "jpg", datetime.timedelta(days=7)
    )
    print("url of predicted image", predicted_file_url)

    # Update url with API
    print("updating with api")
    json_data = {'id': image_id, 'url': predicted_file_url}
    r = requests.patch(os.environ['API_URL']+"image/", json=json_data)
    if r.status_code != 200:
        print("error in updating")
        return
    # Ack message on rabbitmq
    print("send ack to rabbit")
    ch.basic_ack(method.delivery_tag)


load_dotenv()

rabbit_host, rabbit_port = tuple(os.environ["RABBITMQ_HOST"].split(":"))
rabbit_connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=rabbit_host,
        port=rabbit_port,
        virtual_host="/",
        credentials=pika.PlainCredentials(
            os.environ["RABBITMQ_USER"], os.environ["RABBITMQ_PASSWORD"]
        ),
    )
)
rabbit_channel = rabbit_connection.channel()
rabbit_channel.basic_consume(
    queue="q1", auto_ack=False, on_message_callback=process_image
)

ml_model = YOLO("models/best.pt")

print("Start consuming")
rabbit_channel.start_consuming()
