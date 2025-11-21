from dataclasses import dataclass
from .BaseController import BaseController
from .ProjectController import ProjectController
import os
from langchain_community.document_loaders import TextLoader,PyMuPDFLoader
from src.models import ProcessingEnum
# from langchain_text_splitters import RecursiveCharacterTextSplitter, TextSplitter
from typing import List

from ..helpers.chunking import RecursiveTokenChunker

import tiktoken

encoding = tiktoken.get_encoding("cl100k_base")  # same used by GPT-3.5/4

def count_tokens(text: str) -> int:
    return len(encoding.encode(text))

@dataclass
class Document:
    page_content: str
    metadata: dict

class ProcessController(BaseController):

    def __init__(self, project_id: str):
        super().__init__()
        self.project_id = project_id
        self.project_path = ProjectController().get_project_path(project_id=project_id)

    def get_file_extension(self, file_id: str):
        return os.path.splitext(file_id)[-1] #bring file extension

    def get_file_loader(self, file_id: str):
        file_ext = self.get_file_extension(file_id=file_id)
        file_path = os.path.join(
            self.project_path,
            file_id
        )

        if not os.path.exists(file_path):
            return None

        if file_ext == ProcessingEnum.TXT.value:
            return TextLoader(file_path, encoding="utf-8")

        if file_ext == ProcessingEnum.PDF.value:
            return PyMuPDFLoader(file_path)

        return None

    def get_file_content(self, file_id: str):
        loader_file = self.get_file_loader(file_id=file_id)
        if loader_file:
            return loader_file.load()
        return None

    def process_file_content(self, file_content: list, file_id: str,
                             chunk_size: int = 100, overlap_size: int = 20):

        # text_splitter = RecursiveCharacterTextSplitter(
        #     chunk_size=chunk_size,
        #     chunk_overlap=overlap_size,
        #     length_function=len #by default
        # )

        file_content_texts= [
            record.page_content
            for record in file_content
        ]

        file_content_metadata = [
            record.metadata
            for record in file_content
        ]

        #chunks=text_splitter.create_documents(file_content_texts,metadatas=file_content_metadata)


        chunks = self.process_recursive_splitter(
            texts=file_content_texts,
            metadatas=file_content_metadata,
            chunk_size=chunk_size,
            overlap_size=overlap_size,
        )

        return chunks

    def process_simpler_splitter(self, texts: List[str], metadatas: List[dict], chunk_size: int,
                                 splitter_tag: str = "\n"):
        full_text = " ".join(texts)

        # split by splitter_tag
        lines = [doc.strip() for doc in full_text.split(splitter_tag) if len(doc.strip()) > 1]

        chunks = []
        current_chunk = ""

        for line in lines:
            current_chunk += line + splitter_tag
            if len(current_chunk) >= chunk_size:
                chunks.append(Document(
                    page_content=current_chunk.strip(),
                    metadata={}
                ))

                current_chunk = ""

        if len(current_chunk) >= 0:
            chunks.append(Document(
                page_content=current_chunk.strip(),
                metadata={}
            ))

        return chunks


    def process_recursive_splitter(
        self,
        texts: List[str],
        metadatas: List[dict],
        chunk_size: int,
        overlap_size: int = 20,
    ):
        """
        Split text using RecursiveTokenChunker (recursive separators).
        """
        # combine all pages
        full_text = "\n\n".join(texts)

        splitter = RecursiveTokenChunker(
            chunk_size=chunk_size,
            chunk_overlap=overlap_size,
            separators=["\n\n", "\n", ".", "?", "!", " ", ""],
            keep_separator=True,
            length_function=count_tokens,
        )

        raw_chunks = splitter.split_text(full_text)

        chunks: List[Document] = []
        base_metadata = metadatas[0] if metadatas else {}

        for i, chunk_text in enumerate(raw_chunks):
            md = dict(base_metadata)
            md["chunk_id"] = i
            chunks.append(Document(
                page_content=chunk_text.strip(),
                metadata=md,
            ))

        return chunks









