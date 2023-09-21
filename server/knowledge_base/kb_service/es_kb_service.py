
from configs.model_config import (
    SCORE_THRESHOLD
)
from server.knowledge_base.kb_service.base import KBService, SupportedVSType
from server.knowledge_base.utils import KnowledgeFile
from langchain.embeddings.base import Embeddings
from typing import List, Dict, Optional
from langchain.docstore.document import Document

from configs.model_config import kbs_config
from langchain.vectorstores import ElasticsearchStore


class ESKBService(KBService):

    def vs_type(self) -> str:
        return SupportedVSType.ES

    def get_doc_by_id(self, id: str) -> Optional[Document]:
        pass  # uesless

    def do_init(self):
        es_url = kbs_config["es"]["hosts"]
        embeddings = self._load_embeddings()
        self.es = ElasticsearchStore(self.kb_name, embedding=embeddings, es_url=es_url)

    def do_create_kb(self):
        pass  # create when add

    def do_drop_kb(self):
        indices = self.es.client.indices
        if indices.exists(index=self.kb_name):
            indices.delete(index=self.kb_name)

    def do_search(self,
                  query: str,
                  top_k: int,
                  score_threshold: float = SCORE_THRESHOLD,
                  embeddings: Embeddings = None,
                  ) -> List[Document]:
        query = {
            "match": {
                "text": query,
            }
        }
        response = self.es.client.search(index=self.kb_name, query=query, size=top_k)
        hits = [hit for hit in response["hits"]["hits"]]
        docs = [
            (
                Document(
                    page_content=hit["_source"][self.es.query_field],
                    metadata=hit["_source"]["metadata"],
                ),
                hit["_score"],
            )
            for hit in hits
        ]
        return docs

    def do_add_doc(self,
                   docs: List[Document],
                   **kwargs,
                   ) -> List[Dict]:
        ids = self.es.add_documents(docs)
        doc_infos = [{"id": id, "metadata": doc.metadata} for id, doc in zip(ids, docs)]
        return doc_infos

    def do_delete_doc(self,
                      kb_file: KnowledgeFile,
                      **kwargs):
        indices = self.es.client.indices
        if indices.exists(index=self.kb_name):
            query = {"term": {"metadata.source.keyword": kb_file.filepath}}
            self.es.client.delete_by_query(index=self.kb_name, query=query)

    def do_clear_vs(self):
        pass


if __name__ == '__main__':
    faissService = ESKBService("test")
    faissService.add_doc(KnowledgeFile("README.md", "test"))
    faissService.delete_doc(KnowledgeFile("README.md", "test"))
    faissService.do_drop_kb()
    print(faissService.search_docs("如何启动api服务"))
