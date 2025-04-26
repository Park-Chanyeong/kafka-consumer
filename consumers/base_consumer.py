import logging
from confluent_kafka import KafkaError, KafkaException

class BaseConsumer():
    def __init__(self, group_id):
        self.BOOTSTRAP_SERVERS = 'kafka01:9092,kafka02:9092,kafka03:9092'
        self.group_id = group_id
        self.logger = self.get_logger(group_id)

    def get_logger(self, group_id):
        logger = logging.getLogger(group_id)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s]:%(message)s'))
        logger.addHandler(handler)

        return logger

    def callback_on_assign(self, consumer, partition):
        self.logger.info(f'consumer:{consumer}. assigned partition: {partition}')

    def handle_error(self, msg, error):
        if error.code() == KafkaError._PARTITION_EOF:
            # End of partition event
            self.logger.debug(f'End of partition: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
        else:
            # 기타 에러 발생시 종료처리
            self.logger.error(f"Kafka error occurred: {error.str()} "
                              f"on topic {msg.topic()} partition {msg.partition()} "
                              f"at offset {msg.offset()}")
            raise KafkaException(error)