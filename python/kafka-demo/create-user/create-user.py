from kafka import KafkaProducer
import json

USERNAME = "username"
NAME = "name"
MAIL = "mail"
TOPIC = "users"

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'))

def produceToTopic(user):
    print("producing to", TOPIC, "m:", json.dumps(user, ensure_ascii=False))

    producer.send(TOPIC, value=user)

def createUser(username, name, mail):
    user = {}
    user[USERNAME] = username
    user[NAME] = name
    user[MAIL] = mail
    return user


produceToTopic(createUser("user1", "User One", "user.one@mail.com"))
produceToTopic(createUser("user2", "User Two", "user.two@mail.com"))
produceToTopic(createUser("user3", "User Three", "user.three@mail.com"))
produceToTopic(createUser("user4", "User Four", "user.four@mail.com"))

while True:
    username = input("Username:")
    name = input("Name:")
    mail = input("Mail:")

    produceToTopic(createUser(username, name, mail))



