import os

from kafka_mocha.exceptions import KafkaSimulatorBootstrapException
from kafka_mocha.klogger import get_custom_logger
from kafka_mocha.signals import KSignals

try:
    KAFKA_MOCHA_KSIM_ONE_ACK_DELAY = os.environ.get("KAFKA_MOCHA_KSIM_ONE_ACK_DELAY", 1)
    KAFKA_MOCHA_KSIM_ALL_ACK_DELAY = os.environ.get("KAFKA_MOCHA_KSIM_ALL_ACK_DELAY", 3)
except KeyError as err:
    raise KafkaSimulatorBootstrapException(f"Missing Kafka Mocha required variable: {err}") from None

logger = get_custom_logger()


class KafkaSimulator:
    _instance = None
    _is_running = False

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(KafkaSimulator, cls).__new__(cls)
        return cls._instance

    def __init__(self,):
        self.one_ack_delay = KAFKA_MOCHA_KSIM_ONE_ACK_DELAY
        self.all_ack_delay = KAFKA_MOCHA_KSIM_ALL_ACK_DELAY
        logger.info(f"Kafka Simulator initialized")

    def handle_producers(self):
        logger.info("Handle producers has been primed")
        while True:
            received_msgs = yield KSignals.SUCCESS
            logger.info(f"Received messages: {received_msgs}")

    def handle_consumers(self):
        while True:
            yield

    # def run(self):
    #     # handle producers
    #     while True:
    #         try:
    #             self.handle_consumers()
    #         except StopIteration:
    #             print("[KAFKA] Consumers handled")
    #
    #         try:
    #             self.handle_producers()
    #         except StopIteration:
    #             print("[KAFKA] Producers handled")

