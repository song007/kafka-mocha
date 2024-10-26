from datetime import datetime, timedelta

from kafka_mocha.kafka_simulator import KafkaSimulator
from kafka_mocha.klogger import get_custom_logger
from kafka_mocha.models import PMessage
from kafka_mocha.signals import Tick, KSignals

logger = get_custom_logger()


def message_buffer(owner: str, buffer_size: int, buffer_timeout: int = 20):
    """Start off with 1:1 relation to KProducer or KConsumer.

    Does not support custom timestamps (yet).
    """
    logger.info(f"Buffer for {owner} has been primed, size: {buffer_size}, timeout: {buffer_timeout}")
    buffer: list[PMessage] = []  # TODO: consider bytearray() or heap
    buffer_start_time = datetime.now()
    buffer_elapsed_time = 0
    buffer_loop_no = 0
    kafka_simulator = KafkaSimulator()

    producer_protocol = kafka_simulator.handle_producers()
    producer_protocol.send(KSignals.INIT.value)
    res = KSignals.BUFFERED
    while True:
        while len(buffer) < buffer_size:  # TODO: byte size instead of length
            new_msg = yield res
            if isinstance(new_msg, int):
                if new_msg == Tick.DONE:
                    logger.info(f"Buffer for {owner}: received done signal, finishing...")
                    break
                else:
                    logger.info(f"Buffer for {owner}: checking elapsed time: {buffer_elapsed_time}")
                    buffer_elapsed_time += new_msg
                    if buffer_elapsed_time >= buffer_timeout:
                        break
            else:
                buffer.append(new_msg)
                res = KSignals.BUFFERED
        if buffer:
            for msg in buffer:
                msg.timestamp = (
                    buffer_start_time + timedelta(milliseconds=buffer_loop_no * buffer_timeout + buffer_elapsed_time)
                ).timestamp()
            res = producer_protocol.send(buffer)
            logger.info(f"Buffer for {owner}: Kafka response: {res}")
        else:
            logger.info(f"Buffer for {owner}: nothing to send...")
        if res == KSignals.SUCCESS:
            buffer.clear()
        buffer_elapsed_time = 0
        buffer_loop_no += 1