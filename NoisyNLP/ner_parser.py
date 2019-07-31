'''Parser to extract persons, organizations and locations from NER module response'''
import sys
from run_ner import TwitterNER
from twokenize import tokenizeRawTweetText

'''Run NER module'''
def configure():
    configuration = TwitterNER()
    return configuration

def process(tweet=None, configuration=None):
    if tweet is None or configuration is None:
        return (False, [], [], [])
    else:
        #TwitterNER extraction
        tokens = tokenizeRawTweetText(tweet)
        ner_processed = configuration.get_entities(tokens)


        data = {
            'PERSON': [],
            'ORGANIZATION': [],
            'LOCATION': []
        }
        # print 'NER Tweet: \n' + ner_tweet

        '''Run extraction parser'''

        #TwitterNER Extraction
        for ner_token in ner_processed:
            (from_index, to_index, ner_key) = ner_token 
            entity = " ".join(tokens[from_index:to_index])
            data[ner_key].append(entity)

        # print 'People: {0}'.format(extracted['people'])
        # print 'Organizations: {0}'.format(extracted['organizations'])
        # print 'Locations: {0}\n'.format(extracted['locations'])
        return (True, data['PERSON'], data['ORGANIZATION'], data['LOCATION'])