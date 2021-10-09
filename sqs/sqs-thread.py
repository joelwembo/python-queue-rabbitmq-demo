"""Snippet for just-add-water multithreading with AWS SQS service.
Does not currently check for send errors but will one day...
@author Elliot BP (github: ebbp0) <hello@elliot.work>
"""
import json

from multiprocessing import Process, Pipe
from typing import Any

import boto3


class AWSQueue:
    """Represents an AWS SQS queue.
    """

    def __init__(self, queue_url: str):
        self._url = queue_url
        self._sqs = boto3.resource("sqs").Queue(self._url)

    @property
    def url(self) -> str:
        return self._url

    @url.setter
    def url(self, value) -> None:
        """Updates the queue url and automatically refreshes the queue client.
        """
        self._url = value
        self._sqs = boto3.resource("sqs").Queue(self._url)

    def send(self, payload: Any) -> None:
        """Send one or more messages to a queue.
        Args:
            payload: Must be a string, dict, or list of string or dicts of any size.
        
        Raises:
            ValueError if incorrect data type supplied.
        """
        if isinstance(payload, list):
            payload_length = len(payload)
            if payload_length > 10:
                batches: list = [
                    payload[i : i + 10] for i in range(0, payload_length, 10)
                ]
                self._send_many(batches)
                return
            # Each message in a batch must have a unique string ID within that batch
            ids = [x for x in range(payload_length)]
            self._sqs.send_messages(
                Entries=[{"Id": str(ids.pop()), "MessageBody": x} for x in payload]
            )
            return

        if isinstance(payload, str):
            self._sqs.send_message(MessageBody=payload)
            return

        if isinstance(payload, dict):
            self._sqs.send_message(MessageBody=json.dumps(payload))
            return

        raise ValueError("Unsupported payload type: must be list, string or dict.")

    def _send_many(self, batches: list) -> None:
        processes: list = []

        for batch in batches:
            process = Process(target=self.send, args=(batch,))
            processes.append(process)

        for process in processes:
            process.start()

        for process in processes:
            process.join()

        return

    def receive(self, max_number_of_messages: int = 10) -> list:
        """ Receive a list of messages from the queue (min. 10).
        
        Due to the nature of AWS SQS, the exact number of messages received may vary.
        Args:
            max_number_of_messages: an integer with the maximum number of messages,
                will be rounded upwards to nearest ten
            
        Returns:
            A list of messages from the queue, approximately equal to the number specified.
        """
        if max_number_of_messages < 11:
            return self._receive_batch()

        batches = range(0, max_number_of_messages, 10)
        parent_conns: list = []
        processes = []

        for batch in batches:

            parent_conn, child_conn = Pipe()
            parent_conns.append(parent_conn)

            process = Process(target=self._receive_batch, args=(child_conn,))
            processes.append(process)

        for process in processes:
            process.start()

        for process in processes:
            process.join()

        message_lists = []

        for conn in parent_conns:
            message_lists.append(conn.recv())

        # flatten list of lists into 1D list
        return [message for message_list in message_lists for message in message_list]

    def _receive_batch(self, connection=None):
        msgs = self._sqs.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=3)
        payload = [msg.body for msg in msgs]
        delete_ids = [msg.delete() for msg in msgs]
        if connection is None:
            return payload
        connection.send(payload)
        connection.close()
        return
