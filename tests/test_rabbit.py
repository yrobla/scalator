import argparse
import json
import logging
import pika
import sys

log = logging.getLogger("scalator.test_rabbit")
logging.getLogger('pika').setLevel(logging.INFO)

class TestScalator(object):
    def __init__(self):
        self.args = None
        self.addr = 'amqp://testing:test2014@localhost:5672'

    def parse_arguments(self):
        parser = argparse.ArgumentParser(description='TestScalator')
        parser.add_argument('--language', dest='language', default='all', help='specify language to test, all for every languages')
        parser.add_argument('--num_tests', dest='num_tests', default=5, help='speficy the number of messages to send for each language')
        self.args = parser.parse_args()
        if self.args.language == 'all':
            self.languages = ['en', 'nl']
        else:
            self.languages = [self.args.language,]
    
    def get_in_queue_name(self, language):
        if language == 'test':
            return 'Revelator_in_queue_en'
        else:
            return 'Revelator_in_queue_%s' % language

    def get_out_queue_name(self, language):
        if language == 'test':
            return 'Revelator_out_queue_en'
        else:
            return 'Revelator_out_queue_%s' % language

    # publish a message to the specified queue
    def test_queue(self, message_number, language):
        test_id = 'id_%s_%s' % (str(message_number), language)
        print test_id

        with open ("test_message.txt", "r") as myfile:
             body=myfile.read().replace('\n', '')

        properties = pika.BasicProperties(correlation_id=test_id, delivery_mode=2)
        channel = self.connection.channel()
        result = channel.basic_publish(exchange='Test_Revelator_test_queue', routing_key='', body=body, properties=properties, mandatory=True)
    
    def main(self):
        # connect to pika, and send the number of messages specified for each language
        self.connection = pika.BlockingConnection(pika.URLParameters(self.addr))
        for language in self.languages:
            for i in range(0, int(self.args.num_tests)):
                self.test_queue(i,language)
        self.connection.close()


def main():
    test = TestScalator()
    test.parse_arguments()
    return test.main()


if __name__ == "__main__":
    sys.exit(main())

