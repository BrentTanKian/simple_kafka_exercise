from confluent_kafka import Producer
from faker import Faker
import json
import time

fake=Faker()

#Creating the producer and listing available topics to produce to
p=Producer({'bootstrap.servers':'localhost:9092'})
p.list_topics().topics

def receipt(err,msg):
    """
    A callback function that receive error and acknowledgement. In every call, only one will be present, and 
    in the case of error, and error message is printed, and in the case of success, an elaborate statement with the
    timestamp, topic the message is sent to, and the message itself will be printed.
    """
    if err is not None:
        print('Error: {}'.format(err))
    else:
        print('{} : Message on topic {} on partition {} with value of {}'.format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(msg.timestamp()[1]/1000)), 
                                                                                 msg.topic(), msg.partition(), msg.value().decode('utf-8')))
        
for i in range(10):
    #Generates fake data
    data={'name':fake.name(),'age':fake.random_int(min=18, max=80, step=1),'street':fake.street_address(),
          'city':fake.city(),'state':fake.state(),'zip':fake.zipcode()}
    m=json.dumps(data)
    #Poll gets acknowledgements for previous messages
    p.poll(0)
    #Produce to the users topic, referencing the earlier callback function we created
    p.produce('users',m.encode('utf-8'),callback=receipt)
    #Get existing acknowledgements and send them to the receipt function
    p.flush()