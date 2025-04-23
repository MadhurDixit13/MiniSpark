import pickle, logging

logging.basicConfig(level=logging.INFO)

def serialize(obj):
    return pickle.dumps(obj)

def deserialize(bytes_obj):
    return pickle.loads(bytes_obj)