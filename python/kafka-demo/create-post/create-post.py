from kafka import KafkaProducer
import json

TITLE = "title"
USERNAME = "username"
TOPIC = "posts"

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'))

def produceToTopic(post):
    print("producing to", TOPIC, "m:", json.dumps(post, ensure_ascii=False))

    producer.send(TOPIC, value=post)

def createPosts(title, username):
    post = {}
    post[TITLE] = title
    post[USERNAME] = username
    return post

produceToTopic(createPosts("Cool Post Title", "user1"))
produceToTopic(createPosts("AWESOME title!", "user3"))

while True:
    title = input("title:")
    username = input("username:")
    produceToTopic(createPosts(title, username))



