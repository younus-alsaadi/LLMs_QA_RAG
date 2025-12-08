from fastapi import FastAPI, APIRouter, status, Request, HTTPException
from fastapi.responses import JSONResponse
from .schemes.nlp_scheme import PushRequest, SearchRequest
from src.models.ProjectModel import ProjectModel
from src.models.ChunkModel import ChunkModel
from src.models import ResponseSignalEnum
from src.controllers import NLPController
from tqdm.auto import tqdm


import logging
logger = logging.getLogger('uvicorn.error')
logger.setLevel(logging.DEBUG)


nlp_router = APIRouter(
    prefix="/api/v1/nlp",
    tags=["api_v1", "nlp"],
)



#change chunks to vector and insert it in VectorDB
@nlp_router.post("/index/push/{project_id}")
async def index_project(request: Request, project_id: int, push_request: PushRequest):
    container = request.app.state.container

    project_model = await ProjectModel.create_instance(
        db_client=container.db_client
    )
    chunk_model = await ChunkModel.create_instance(
        db_client=container.db_client
    )

    project = await project_model.get_project_or_create_one(
        project_id=project_id
    )

    if not project:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "signal": ResponseSignalEnum.PROJECT_NOT_FOUND_ERROR.value
            }
        )

    nlp_controller = NLPController(
        vectordb_client=container.vectordb_client,
        generation_client=container.generation_client,
        embedding_client=container.embedding_client,
        template_parser=container.template_parser
    )

    has_records=True
    page_number = 1
    inserted_items_count = 0
    idx=0

    # create collection if not exists
    collection_name = nlp_controller.create_collection_name(project_id=project.project_id)

    _ = await container.vectordb_client.create_collection(
        collection_name=collection_name,
        embedding_size=container.embedding_client.embedding_dimensions_size,
        do_reset=push_request.do_reset,
    )

    total_chunks_count = await chunk_model.get_total_chunks_count(project_id=project.project_id)
    pbar = tqdm(total=total_chunks_count, desc="Vector Indexing", position=0)

    while has_records:
        page_chunks=await chunk_model.get_project_chunks(project_id=project.project_id,page_no=page_number)
        if len(page_chunks):
            page_number+=1

        if not page_chunks or len(page_chunks)==0:
            has_records=False
            break

        chunks_ids_per_page=[c.chunk_id for c in page_chunks ]
        idx+=len(page_chunks)


        is_inserted=await nlp_controller.index_into_vector_db(
            project=project,
            chunks=page_chunks,
            chunks_ids=chunks_ids_per_page
        )


        if not is_inserted:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={
                    "signal": ResponseSignalEnum.INSERT_INTO_VECTORDB_ERROR.value
                }
            )
        pbar.update(len(page_chunks))
        inserted_items_count+=len(page_chunks)



    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "signal": ResponseSignalEnum.INSERT_INTO_VECTORDB_SUCCESS.value,
            "inserted_items_count": inserted_items_count
        }
    )

#Get Info about the collection in vectorDB
@nlp_router.get("/index/info/{project_id}")
async def get_project_index_info(request: Request, project_id: int):
    container = request.app.state.container
    project_model = await ProjectModel.create_instance(
        db_client=container.db_client
    )

    project = await project_model.get_project_or_create_one(
        project_id=project_id
    )


    nlp_controller = NLPController(
        vectordb_client=container.vectordb_client,
        generation_client=container.generation_client,
        embedding_client=container.embedding_client,
        template_parser=container.template_parser
    )


    collection_info =await nlp_controller.get_vector_db_collection_info(project=project)

    return JSONResponse(
        content={
            "signal": ResponseSignalEnum.VECTORDB_COLLECTION_RETRIEVED.value,
            "collection_info": collection_info
        }
    )

@nlp_router.post("/index/search/{project_id}")
async def search_index(request: Request, project_id: int, search_request: SearchRequest):
    container = request.app.state.container
    project_model = await ProjectModel.create_instance(db_client=container.db_client)
    project = await project_model.get_project_or_create_one(project_id=project_id)

    nlp_controller = NLPController(
        vectordb_client=container.vectordb_client,
        generation_client=container.generation_client,
        embedding_client=container.embedding_client,
        template_parser=container.template_parser,
    )

    try:
        results, usage_data =await nlp_controller.search_vector_db_collection(
            project=project,
            text=search_request.text,
            limit=search_request.limit
        )
    except HTTPException as e:
        if e.status_code == status.HTTP_409_CONFLICT:
            return JSONResponse(
                status_code=e.status_code,
                content={
                    "signal": "VECTORDB_CONFIG_MISMATCH",
                    "message": e.detail
                }
            )
        return JSONResponse(
            status_code=e.status_code,
            content={
                "signal": ResponseSignalEnum.VECTORDB_SEARCH_ERROR.value,
                "message": e.detail
            }
        )

    if not results:
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "signal": ResponseSignalEnum.VECTORDB_SEARCH_SUCCESS.value,
                "results": []
            }
        )

    return JSONResponse(
        content={
            "signal": ResponseSignalEnum.VECTORDB_SEARCH_SUCCESS.value,
            "results": [r.dict() for r in results],
            "usage_data": usage_data
        }
    )

#Take the similar chunk and send it to the LLM to generate an answer
@nlp_router.post("/index/answer/{project_id}")
async def answer_rag_from_user(request: Request, project_id: int, search_request: SearchRequest):
    container = request.app.state.container
    project_model = await ProjectModel.create_instance(
        db_client=container.db_client
    )

    project = await project_model.get_project_or_create_one(
        project_id=project_id
    )

    nlp_controller = NLPController(
        vectordb_client=container.vectordb_client,
        generation_client=container.generation_client,
        embedding_client=container.embedding_client,
        template_parser=container.template_parser,
    )

    answer_from_generation_model, full_prompt, chat_history, total_tokens, cost= await nlp_controller.answer_rag_question(
        project=project,
        query=search_request.text,
        limit=search_request.limit,
    )

    logger.debug("=" * 20)
    logger.debug("")
    logger.debug("=" * 20)

    if not answer_from_generation_model:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "signal": ResponseSignalEnum.RAG_ANSWER_ERROR.value
            }
        )

    return JSONResponse(
        content={
            "signal": ResponseSignalEnum.RAG_ANSWER_SUCCESS.value,
            "answer": answer_from_generation_model,
            "full_prompt": full_prompt,
            "chat_history": chat_history,
            "total_tokens":total_tokens,
            "cost": cost
        })


