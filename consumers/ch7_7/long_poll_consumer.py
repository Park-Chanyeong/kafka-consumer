
from confluent_kafka import Consumer
from confluent_kafka import KafkaException
from consumers.base_consumer import BaseConsumer
import pandas as pd
import json
import time
import random


class LongPollConsumer(BaseConsumer):
    def __init__(self, group_id):
        super().__init__(group_id)
        self.topics = ['apis.seouldata.rt-bicycle']

        conf = {'bootstrap.servers': self.BOOTSTRAP_SERVERS,
                'group.id': self.group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': 'false',
                'partition.assignment.strategy':'cooperative-sticky',   # range, roundrobin, cooperative-sticky
                'max.poll.interval.ms':'45000',                         # Default: 5분, setting: 45초
                'session.timeout.ms':'10000'                            # Default: 45초, setting: 10초
                }

        self.consumer = Consumer(conf)
        self.consumer.subscribe(self.topics, on_assign=self.callback_on_assign)


    def poll(self):
        try:
            while True:
                msg_lst = self.consumer.consume(num_messages=100)
                if msg_lst is None or len(msg_lst) == 0: continue

                self.logger.info(f'message count:{len(msg_lst)}')
                for msg in msg_lst:
                    error = msg.error()
                    if error:
                        self.handle_error(msg, error)

                # 로직 처리 부분
                # Kafka 레코드에 대한 전처리, Target Sink 등 수행
                self.logger.info(f'message 처리 로직 시작')
                msg_val_lst = [json.loads(msg.value().decode('utf-8')) for msg in msg_lst]
                df = pd.DataFrame(msg_val_lst)
                print(df[:10])


                self.logger.info(f'message 처리 로직 완료, Async Commit 후 2초 대기')
                # 로직 처리 완료 후 Async Commit 수행
                self.consumer.commit(asynchronous=True)
                self.logger.info(f'Commit 완료')
                rand_sleep_sec = random.choice([30,30,30,30,30,30,30,30,30,60])        # 리스트 값 중 하나 임의 선택, 10번 중 한번 꼴로 60초 수행
                self.logger.info(f'랜덤 지연 시간: {rand_sleep_sec}초')

                # sleep 으로 다음 poll 수행을 늦춤
                time.sleep(rand_sleep_sec)

        except KafkaException as e:
            self.logger.exception("Kafka exception occurred during message consumption")

        except KeyboardInterrupt:  # Ctrl + C 눌러 종료시
            self.logger.info("Shutting down consumer due to keyboard interrupt.")

        finally:
            self.consumer.close()
            self.logger.info("Consumer closed.")


if __name__ == '__main__':
    long_poll_consumer = LongPollConsumer('long_poll_consumer')
    long_poll_consumer.poll()