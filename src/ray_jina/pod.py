import ray
from ray_jina.utils import serialize, deserialize
from jina import __default_endpoint__
from typing import List, Callable, Dict, Optional, Tuple, Union

class Pod(object):
    def __init__(self, executor, **kwargs):
        self.executor = deserialize(executor)()
        self.dependents = []
    
    def uses(self, deps: Union[List[str], str]):
        if(isinstance(deps, list)):
            for dep in deps:
                self.dependents.extend(deps)
        else:
            self.dependents.append(deps)
            
    #TODO: should probably pass these arguments as kwargs
    def handle(self, uri: str, doc):
        unserialized = deserialize(doc)
        self.executor(uri, **{"docs": unserialized})
        serialized = serialize(unserialized)
        
        if len(self.dependents) == 0:
            object_ref = ray.put(serialized)
            return object_ref
        
        for dep in self.dependents:
            pod = ray.get_actor(dep)
            return ray.get(pod.handle.remote(uri, serialized))
                
        