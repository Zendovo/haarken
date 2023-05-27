from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from listnr.models import Task
from .tasks import fetch_comments
import json

# Create your views here.
class TaskView(APIView):

    def post(self, request):
        data = request.body.decode('utf-8')
        data = json.loads(data)

        email = data['email']
        video_id = data['video_id']
        description = data['description']

        task = Task.objects.create(email=email, video_id=video_id, status='CREATED', description=description)
        
        celery_task = fetch_comments.delay(task.id)
        task.fetch_comments_id = celery_task.id

        return Response({ 'message': 'queued task' }, status=status.HTTP_200_OK)
