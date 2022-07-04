import ray
import pickle5 as pickle
from jina import Document, DocumentArray, Flow
from jina.logging.profile import TimeContext

from ray_jina import Pipeline, Pod
from ray_jina.executors import TestExecutor, TestPreprocImg, TestEmbedImg, TestMatchImg

if not ray.is_initialized():    
    ray.init()

def preproc(d: Document):
    return (d.load_uri_to_image_blob()  # load
             .set_image_blob_normalization()  # normalize color 
             .set_image_blob_channel_axis(-1, 0))  # switch color axis


def test_single_pod():
    docs = DocumentArray.from_files('../img/000*.jpg').apply(preproc)
    
    q = (Document(uri='../img/00021.jpg')  # build query image & preprocess
    .load_uri_to_image_blob()
    .set_image_blob_normalization()
    .set_image_blob_channel_axis(-1, 0))

    serialized = pickle.dumps(TestExecutor)
    Handler = ray.remote(Pod)
    handler_actor = Handler.remote(serialized)
    
    docs = pickle.dumps(docs)

    ray.get(handler_actor.handle.remote("/index", docs))
    
    da = pickle.dumps(DocumentArray([q]))
    doc_ref = ray.get(handler_actor.handle.remote("/search", da))
    
    da = pickle.loads(ray.get(doc_ref))

    assert len(da[0].matches) >= 0

def test_chained_pods():
    # docs = DocumentArray.from_files('../img/000*.jpg')
    docs = DocumentArray.from_files('../img/00*.jpg')

    # da = DocumentArray.from_files('../img/00021.jpg')
    da = DocumentArray.from_files('../img/000*.jpg')
        
    executors = [TestPreprocImg, TestEmbedImg, TestMatchImg]
    
    pods = [ray.remote(Pod) for _ in range(len(executors))]
    pod_actors = [None] * len(pods)
    
    for index, executor in enumerate(executors):
        serialized_pod = pickle.dumps(executor)
        pod_actors[index] = pods[index].options(name=executor.__name__).remote(serialized_pod)
    
    pod_actors[0].uses.remote(executors[1].__name__)
    pod_actors[1].uses.remote(executors[2].__name__)
    
    docs = pickle.dumps(docs)
    da = pickle.dumps(da)
    
    ray.get(pod_actors[0].handle.remote("/index", docs))
    # da = pickle.loads(ray.get(pod_actors[0].handle.remote("/search", da)))
    
    with TimeContext("ray jina"):
        da_ref = ray.get(pod_actors[0].handle.remote("/search", da))
        da = pickle.loads(ray.get(da_ref))
    
    #Index documents embeddings
    # for i, pod_actor in enumerate(pod_actors):
    #     docs = pickle.dumps(docs)
    #     if i == 2:
    #         pod_actor.handle.remote("/index", docs)
    #     else:
    #         docs = pickle.loads(ray.get(pod_actor.handle.remote("/index", docs)))
    #     print(i)

    
    #Search for image
    # for pod_actor in pod_actors:
    #     da = pickle.dumps(da)
    #     da = pickle.loads(ray.get(pod_actor.handle.remote("/search", da)))
    
    assert len(da) >= 0
        
    # for m in q.matches:
    #     # m.set_image_blob_channel_axis(0, -1).set_image_blob_inv_normalization()
    #     print(m.uri, m.scores["cosine"].value, "\n")


if __name__ == '__main__':
    # test_chained_pods()
    test_single_pod()