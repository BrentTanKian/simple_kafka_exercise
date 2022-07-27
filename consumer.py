from confluent_kafka import Consumer

#Create consumer named python-consumer which consumes from the beginning (earliest offset)
c=Consumer({'bootstrap.servers': 'localhost:9092','group.id':'python-consumer',
            'auto.offset.reset':'earliest'})

c.list_topics()

c.subscribe(['users'])

#Creates an infinite loop that listens forever
while True:
    msg=c.poll(1.0) #timeout
    #If no messages yet, continue listening for one
    if msg is None:
        continue
    #If error, print error message and continue listening
    if msg.error():
        print('Error: {}'.format(msg.error()))
        continue
    data=msg.value().decode('utf-8')
    #Print contents of consumed message
    print(data)
c.close()