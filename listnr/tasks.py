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
        logger.info("Invalid Task ID supplied")
        return

    task = task[0]

    try:
        pipeline = YoutubePipeline(task.video_id, task.description)

        task.all_comments_data = json.dumps(pipeline.all_comments_data)
        task.status = "FETCHED_COMMENTS"
        task.save()

        analyse_comments.delay(task_id)
    except Exception as e:
        # Handle errors later
        task.status = "FAILED_FETCH_COMMENTS"
        task.save()
        logger.error(e)


@shared_task
def analyse_comments(task_id):
    task = Task.objects.filter(pk=task_id)

    if len(task) == 0:
        logger.info("Invalid Task ID supplied")
        return

    task = task[0]

    try:
        pipeline = YoutubePipeline(
            task.video_id, task.description, json.loads(task.all_comments_data)
        )
        df = asyncio.run(pipeline.get_analyses())

        task.analysed_comments = json.dumps(df)
        task.status = "ANALYSED_COMMENTS"
        task.save()

        parse_analysis.delay(task_id)
    except Exception as e:
        # Handle errors later
        task.status = "FAILED_ANALYSE_COMMENTS"
        task.save()
        logger.error(e)


@shared_task
def parse_analysis(task_id):
    task = Task.objects.filter(pk=task_id)

    if len(task) == 0:
        logger.info("Invalid Task ID supplied")
        return

    task = task[0]

    try:
        pipeline = YoutubePipeline(task.video_id, task.description, json.loads(task.all_comments_data))
        pipeline.analysis_df = json.loads(task.analysed_comments)
        print(1)
        pipeline.parse_analyses()

        print(2)
        task.status = "PARSED_ANALYSIS"
        task.save()
    except Exception as e:
        # Handle errors later
        task.staus = "FAILED_PARSE_ANALYSIS"
        task.save()
        logger.error(e)
