from ray_jina import Pipeline, Pod
from jina import Document, DocumentArray
from ray_jina.executors import TestExecutor, TestPreprocImg, TestEmbedImg, TestMatchImg

def test_pipeline_single_pod():
    def preproc(d: Document):
        return (d.load_uri_to_image_blob()  # load
             .set_image_blob_normalization()  # normalize color
             .set_image_blob_channel_axis(-1, 0))  # switch color axis

    docs = DocumentArray.from_files('../img/000*.jpg').apply(preproc)
    da = DocumentArray.from_files('../img/00021.jpg').apply(preproc)

    p = Pipeline([TestExecutor])
    p.deploy()

    p.post("/index", docs, returns=False)
    da = p.post("/search", da)

    assert len(da[0].matches) > 0

def test_pipeline_multiple_pods():
    docs = DocumentArray.from_files('../img/000*.jpg')
    da = DocumentArray.from_files('../img/00021.jpg')

    p = Pipeline([TestPreprocImg, TestEmbedImg, TestMatchImg])
    p.deploy()

    p.post("/index", docs, returns=False)
    da = p.post("/search", da)

    assert len(da[0].matches) > 0


if __name__ == '__main__':
    test_pipeline_single_pod()
