import chomp_config_load as cfg
from multiprocessing import Queue, Lock
from datetime import datetime
from sys import argv
import json
from time import sleep
import redis


if __name__ == '__main__':
    # main function and runner

    DataQ = Queue()
    PubQ = Queue()
    LOCK = Lock()

    if len(argv) < 2:
        appconfigfile = cfg.getconfigfile()
    else:
        appconfigfile = cfg.getconfigfile(argv[1])

    appconfig = cfg.load_config(appconfigfile)

    print(appconfig)

    a = redis.Redis(host=appconfig['redishost'], port=appconfig['redisport'], db=0)
    subscriber = a.pubsub()
    subscriber.subscribe(appconfig['redis_corrlog_topic'])

    while True:
        message = subscriber.get_message()
        if isinstance(message, type(None)):
            if DataQ.empty():
                print(datetime.utcnow(), "sleeping 10 seconds")
                sleep(10)
            else:
                print("\n")
                print(datetime.utcnow())
                print(json.dumps(DataQ.get(), indent=2))
            continue
        else:
            try:
                temp = json.loads(message['data'].decode("utf-8"))
                DataQ.put(temp)
            except Exception:
                pass
