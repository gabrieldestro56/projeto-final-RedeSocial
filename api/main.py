from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from broker import Broker
from pydantic import BaseModel
import time
import uuid
from contextlib import asynccontextmanager
import json

app = FastAPI()
broker = Broker()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await broker.connect()
    yield
    await broker.disconnect()

app = FastAPI(lifespan=lifespan)

# Apply CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (you can restrict this to specific domains)
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

class Post(BaseModel):
    username: str
    content: str
    timestamp: str | None = None

@app.post("/post")
async def create_post(post: Post):
    # Add timestamp to the post
    post.timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    
    # Prepare the message with operation type and data
    correlation_id = str(uuid.uuid4())  # Unique ID for the RPC call
    message = {
        "operation": "criar_post",
        "data": post.model_dump()
    }
    
    # Publish the message and wait for the response
    response = await broker.rpc_publish("FILA_MENSAGEM", message, correlation_id)
    
    return response

@app.get("/posts")
async def get_posts():
    # Prepare the message with operation type
    correlation_id = str(uuid.uuid4())  # Unique ID for the RPC call
    message = {
        "operation": "get_posts",
        "data": {}
    }
    
    # Publish the message and wait for the response
    response = await broker.rpc_publish("FILA_MENSAGEM", message, correlation_id)
    response = json.loads(response)  # Parse the string into JSON
    return response

class FollowRequest(BaseModel):
    follower: str
    following: str
    timestamp: str | None = None

@app.post("/seguir")
async def follow_user(data: FollowRequest):
    data.timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    
    correlation_id = str(uuid.uuid4())
    message = {
        "operation": "seguir",
        "data": data.model_dump()
    }
    
    response = await broker.rpc_publish("FILA_MENSAGEM", message, correlation_id)
    return response

@app.get("/get-seguidores/{username}")
async def get_follows(username: str):
    correlation_id = str(uuid.uuid4())
    message = {
        "operation": "get_seguidores",
        "data": {"username": username}
    }

    response = await broker.rpc_publish("FILA_MENSAGEM", message, correlation_id)
    response = json.loads(response)
    return response

@app.get("/seguidores-mutuos/{username}")
async def get_mutual_follows(username: str):
    correlation_id = str(uuid.uuid4())
    message = {
        "operation": "get_seguidores_em_comum",
        "data": {"username": username}
    }
    response = await broker.rpc_publish("FILA_MENSAGEM", message, correlation_id)
    return json.loads(response)

class Mensagem(BaseModel):
    sender: str
    receiver: str
    content: str
    timestamp: str | None = None

@app.post("/enviar-mensagem")
async def enviar_mensagem(mensagem: Mensagem):
    mensagem.timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    correlation_id = str(uuid.uuid4())
    message = {
        "operation": "enviar_mensagem",
        "data": mensagem.model_dump()
    }

    response = await broker.rpc_publish("FILA_MENSAGEM", message, correlation_id)
    return response

@app.get("/get-historico/{sender}/{receiver}")
async def get_historico(sender: str, receiver: str):
    correlation_id = str(uuid.uuid4())
    message = {
        "operation": "get_historico",
        "data": {
            "sender": sender,
            "receiver": receiver
        }
    }

    response = await broker.rpc_publish("FILA_MENSAGEM", message, correlation_id)
    return json.loads(response)

@app.post("/desligar-coordenador")
async def shutdown_coordinator():
    correlation_id = str(uuid.uuid4())
    message = {
        "operation": "desligar_coordenador",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    }

    response = await broker.rpc_publish("FILA_MENSAGEM", message, correlation_id)
    return response