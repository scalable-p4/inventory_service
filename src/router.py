from databases.interfaces import Record
from fastapi import APIRouter, BackgroundTasks, Depends, Response, status
from src.database import database
from dotenv import load_dotenv
from celery import Celery
from sqlalchemy import insert, select
from celery.result import AsyncResult
router = APIRouter()

@router.get("/hello", status_code=status.HTTP_200_OK)
async def say_hi():
    return {"message": "Hello World inventory"}
