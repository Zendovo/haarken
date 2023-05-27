from __future__ import absolute_import, unicode_literals

from celery import shared_task
from celery.utils.log import get_task_logger
import json
from listnr.models import Task
from .Pipeline import YoutubePipeline
from asgiref.sync import async_to_sync
import asyncio

logger = get_task_logger(__name__)


@shared_task
def fetch_comments(task_id):
    task = Task.objects.filter(pk=task_id)

    if len(task) == 0:
        logger.info('Invalid Task ID supplied')
        return
    
    task = task[0]

    try:
        pipeline = YoutubePipeline(task.video_id, task.description)

        task.all_comments_data = json.dumps(pipeline.all_comments_data)
        task.save()

        analyse_comments.delay(task_id)
    except Exception as e:
        # Handle errors later
        logger.error(e)


@shared_task
def analyse_comments(task_id):
    task = Task.objects.filter(pk=task_id)

    if len(task) == 0:
        logger.info('Invalid Task ID supplied')
        return
    
    task = task[0]

    try:
        pipeline = YoutubePipeline(task.video_id, task.description, json.loads(task.all_comments_data))
        df = asyncio.run(pipeline.get_analyses())
        
        task.analysed_comments = json.dump(df)
        task.save()

        # TODO:
        # parse anaylsis task
    except Exception as e:
        # Handle errors later
        logger.error(e)
