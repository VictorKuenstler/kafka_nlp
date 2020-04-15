import re
from collections import Counter

# to keep memory usage small for testing, for production use -1
MAX_CHAR_NUM = 1000000


def preprare_document(document):
    document = document.decode()[:MAX_CHAR_NUM]
    return re.sub(r'\n', '', document)


def process_document(document, nlp, allowed_entities):
    if type(document) is not str:
        doc = nlp(preprare_document(document))
    else:
        doc = nlp(document)

    ents = list(filter(lambda x: x.label_ in allowed_entities, doc.ents))
    num_ents = len(ents)
    ents = list(map(lambda x: x.text, ents))
    tf = Counter(ents)

    return tf, num_ents
