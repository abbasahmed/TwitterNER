import redis
import json
import sys
import datetime
import threading
import ner_parser
import multiprocessing


def redis_push(collection, doc, mustExist=True):
    if collection is None or doc is None:
        return False
    elif client.exists(collection):
        client.lpush(collection, doc)
        return True
    elif not mustExist:
        client.lpush(collection, doc)
        return True
    else:
        return False


def redis_pop(collection):
    if collection is None:
        return None
    elif client.exists(collection):
        return client.rpop(collection)
    else:
        return None


def ner_process(producer=None, collection=None, configuration=None):
    if producer is None or collection is None:
        print('TEST ERROR')
        return
    print('[' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '] Classifying tweets and extracting entities')
    while (True):
        doc = redis_pop(collection=producer)
        if doc is None:
            break
            # continue
        data = json.loads(doc)
        tweet = data['tweet_text']
        (status, people, organizations, locations) = ner_parser.process(
            tweet=tweet,
            configuration=configuration
        )
        data['people'] = people
        data['organization'] = organizations
        data['location'] = locations
        if status is True:
            updated_doc = json.dumps(data)
            redis_push(collection=collection, doc=updated_doc, mustExist=False)


# Initiate script
if len(sys.argv) < 3:
    print('Usage:\npython2 ner_module.py [aidr_redis_list_name] [inserter_redis_list_name]')
    sys.exit(1)


# List names
producer = sys.argv[1]  # aidr
collection = sys.argv[2]  # inserter

# Redis instances
client = redis.StrictRedis(host='localhost', port=6379, db=0)


print('[' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '] Configuring NER module')

# THIS IS FOR SINGLE THREAD/PROCESS
config = ner_parser.configure()

# THIS IS FOR MULTITHREADING OR MULTIPROCESSING
# Loading 10 models

# configs = [ner_parser.configure(), ner_parser.configure(), ner_parser.configure(), ner_parser.configure(),
#            ner_parser.configure(), ner_parser.configure(), ner_parser.configure(), ner_parser.configure(),
#            ner_parser.configure(), ner_parser.configure()]

print('[' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '] Configurations complete')

start = datetime.datetime.now()

print('[' + start.strftime('%Y-%m-%d %H:%M:%S') + '] NER Processing initiated')


############################################
# # Multi-Threading
# max_threads = 10
# threads = []
# for i in range(0, max_threads):
#     thread = threading.Thread(target=ner_process, args=(producer, collection, configs[i]))
#     threads.append(thread)
#     thread.start()

# for thread in threads:
#     thread.join()

############################################
# # Multi-Processing
# max_process = 10
# processes = []
# for i in range(0, max_process):
#     process = threading.Thread(target=ner_process, args=(producer, collection, configs[i]))
#     processes.append(process)
#     process.start()

# for process in processes:
#     process.join()


############################################
# For single process
ner_process(producer=producer, collection=collection, configuration=config)

end = datetime.datetime.now()
print('[' + end.strftime('%Y-%m-%d %H:%M:%S') + '] NER process complete')
print('Time elapsed: ' + str(end - start) + '\n')
