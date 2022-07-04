from jina import Executor, Document, DocumentArray, requests
import torchvision

class TestExecutor(Executor):
    _da = DocumentArray()
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.model = torchvision.models.resnet50(pretrained=True) 

    @requests(on='/index')
    def index(self, docs: DocumentArray, **kwargs):
        docs.embed(self.model)
        self._da.extend(docs)
        docs.clear()  # clear content to save bandwidth

    @requests(on='/search')
    def search(self, docs: DocumentArray, **kwargs):
        docs.embed(self.model)
        docs.match(self._da)
        for d in docs.traverse_flat('r,m'):  # only require for visualization
            d.convert_uri_to_datauri()  # convert to datauri
            d.pop('embedding', 'blob')  # remove unnecessary fields for save bandwidth

class TestEmbedImg(Executor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.model = torchvision.models.resnet50(pretrained=True)        

    @requests
    def foo(self, docs: DocumentArray, **kwargs):
        docs.embed(self.model)

class TestPreprocImg(Executor):
    @requests
    def foo(self, docs: DocumentArray, **kwargs):
        for d in docs:
            (d.load_uri_to_image_blob()  # load
             .set_image_blob_normalization()  # normalize color
             .set_image_blob_channel_axis(-1, 0))  # switch color axis
            
class TestMatchImg(Executor):
    _da = DocumentArray()

    @requests(on='/index')
    def index(self, docs: DocumentArray, **kwargs):
        self._da.extend(docs)
        docs.clear()  # clear content to save bandwidth

    @requests(on='/search')
    def foo(self, docs: DocumentArray, **kwargs):
        docs.match(self._da)
        for d in docs.traverse_flat('r,m'):  # only require for visualization
            d.convert_uri_to_datauri()  # convert to datauri
            d.pop('embedding', 'blob')  # remove unnecessary fields for save bandwidth