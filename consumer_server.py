import logging

from kafka import KafkaConsumer
from json import loads
import tornado.ioloop
import tornado.template
import tornado.web
from tornado import gen
from confluent_kafka import Consumer

logger = logging.getLogger(__name__)


class KafkaConsumerServer(KafkaConsumer):
    """Defines the base kafka consumer class"""
    def __init__(
        self,
        topic_name,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        
        self.topic_name = topic_name
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

      
        # TODO: Create the Consumer, using the appropriate type.

        self.broker_properties = {
            "bootstrap.servers": "localhost:9092",
            "group.id": f'{self.topic_name}_group',
            "default.topic.config": {"auto.offset.reset": "earliest"}
        }

        self.consumer = Consumer(
            self.broker_properties
        )            

        #
        #
        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        #
        #
        self.consumer.subscribe([self.topic_name])

   

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        try:
            message = self.consumer.poll(self.consume_timeout)
        except error:
            logger.error(f'Could not consume message from {self.topic_name}')
            return 0
       
        if message is None:
            logger.debug('Message is none')
            return 0
        elif message.error() is not None:
            logger.debug('Message can not be consumed. Error caused by: {message.error()}')
            return 0
        else:
            print(f'{message.value()}')
            return 1

if __name__ == "__main__":
    consumer = KafkaConsumerServer("com.police.dept.crime.stats",offset_earliest=True)
    try:
        logger.info(
            "Consuming data"
        )
        
        tornado.ioloop.IOLoop.current().spawn_callback(consumer.consume)
        tornado.ioloop.IOLoop.current().start()

    except KeyboardInterrupt as e:
        logger.info("shutting down server")
        tornado.ioloop.IOLoop.current().stop()
        consumer.close()