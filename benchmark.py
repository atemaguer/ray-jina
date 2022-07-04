import ray
from jina import Document, DocumentArray, Flow
from jina.logging.profile import TimeContext

from ray_jina import Pipeline
from ray_jina.executors import TestExecutor, TestPreprocImg, TestEmbedImg, TestMatchImg

docs = DocumentArray.from_files('./img/00*.jpg')
da = DocumentArray.from_files('./img/000*.jpg')

def benchmark_flow():
    f = Flow(port_expose=12345, protocol='http').add(uses=TestPreprocImg).add(uses=TestEmbedImg).add(uses=TestMatchImg)
    with f:
        f.post('/index', docs, show_progress=True, request_size=8)

        with TimeContext("Benchmarking jina Flow"):
            f.post("/search", da)

def benchmark_pipeline():
    p = Pipeline([TestPreprocImg, TestEmbedImg, TestMatchImg])
    p.deploy()

    p.post("/index", docs, returns=False)

    with TimeContext("Benchmarking ray_jina Pipeline"):
        p.post("/search", da)


if __name__ == '__main__':
    benchmark_flow()
    benchmark_pipeline()
