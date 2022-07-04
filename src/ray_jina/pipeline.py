import ray
from ray_jina.utils import serialize, deserialize
from ray_jina.pod import Pod
from jina import Executor

import pickle5 as pickle

if not ray.is_initialized():
    ray.init()
    
class Pipeline(object):
    def __init__(self, executors, **kwargs):
        """
        TODOS:
            - Add bookkeeping variables e.g check whether pipeline is deployed
        """
        self.executors = executors
        self.pods = None
        self.pod_actors = [None] * len(self.executors)
        
    @property
    def is_ready():
        pass
    
    def post(self, uri, inputs, returns=True, *args, **kwargs):
        """
        TODOS:
            - perform some error checking here. There should be some executors already deployed.
            - check for valid path/url. validate uri format.
            - validate that the inputs is instance of DocumentArray. if not wrap in DocumentArray
            - validate that DocumentArray contains documents.
        """
        
        docs = serialize(inputs)
        
        object_ref = ray.get(self.pod_actors[0].handle.remote(uri, docs))
        
        if returns:
            result = deserialize(ray.get(object_ref))
            return result
    
    def deploy(self):
        """
        TODOS:
            - should perform some error checking here e.g redeployment, empty executor list, etc.
        """
        self.pods = [ray.remote(Pod) for _ in range(len(self.executors))]
        
        for index, executor in enumerate(self.executors):
            serialized_pod = serialize(executor)
            self.pod_actors[index] = self.pods[index].options(name=executor.__name__).remote(serialized_pod)
        
        for i in range(len(self.pod_actors) - 1):
            self.pod_actors[i].uses.remote(self.executors[i+1].__name__)
    

