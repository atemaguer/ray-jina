import pickle5 as pickle

def deserialize(serialized_data):
    return pickle.loads(serialized_data)
    
def serialize(data):
    return pickle.dumps(data)