from kafka import KafkaProducer
import json

USERNAME = "username"
SUBSCRIBE_TO = "subscribeto"

TOPIC = "subscriptions"

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'))

def produceToTopic(subscription):
    print("producing to", TOPIC, "m:", json.dumps(subscription, ensure_ascii=False))

    producer.send(TOPIC, value=subscription)


def createSubscription(username, subscribeto):
    subscription = {}
    subscription[USERNAME] = username
    subscription[SUBSCRIBE_TO] = subscribeto
    return subscription

produceToTopic(createSubscription("user1", "user1"))
produceToTopic(createSubscription("user1", "user2"))
produceToTopic(createSubscription("user1", "user3"))
produceToTopic(createSubscription("user1", "user4"))
produceToTopic(createSubscription("user2", "user3"))
produceToTopic(createSubscription("user4", "user3"))

while True:
    username = input("Username:")
    subscribeto = input("Subscribe to:")

    produceToTopic(createSubscription(username, subscribeto))



