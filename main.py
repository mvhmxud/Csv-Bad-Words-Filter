import csv
import re
import time
from queue import Queue
from threading import Thread
import pandas as pd

SENTINEL = object()


class Producer(Thread):
    def __init__(self, sharedqueue):
        Thread.__init__(self)
        self.sharedqueue = sharedqueue

    def run(self):
        counter = 1
        for dfChunk in pd.read_csv('files.csv', chunksize=100, usecols=['email'], dtype=str):
            self.sharedqueue.put(dfChunk.to_string(header=False, index=False))
        self.sharedqueue.put(SENTINEL)


class Consumer(Thread):
    def __init__(self, sharedqueue):
        Thread.__init__(self)
        self.sharedQueue = sharedqueue

    def run(self):
        # open and create the files and set the writers
        badWords = open('bad_words.txt').read()
        badRecords = open('badRecords.csv', 'w')
        badRecordsWriter = csv.writer(badRecords)
        healthyRecords = open('healthyRecords.csv', 'w')
        healthyRecordsWriter = csv.writer(healthyRecords)
        while True:
            chunk = self.sharedQueue.get()
            if chunk is SENTINEL:
                break
            listofEmails = str(chunk).splitlines()
            listofBadWords = badWords.splitlines()
            for email in listofEmails:
                is_bad = False
                for word in listofBadWords:
                    match = re.findall(word, email)
                    if not len(match) == 0:
                        is_bad = True
                if is_bad:
                    badRecordsWriter.writerow([email])
                elif not is_bad:
                    healthyRecordsWriter.writerow([email])


def main():
    start_time = time.time()
    sharedqueue = Queue()
    producer_thread = Producer(sharedqueue)
    producer_thread.start()

    consumer = Consumer(sharedqueue)
    consumer.start()

    consumer.join()
    print(f'Finished within  {time.time() - start_time}')


if __name__ == '__main__':
    main()
