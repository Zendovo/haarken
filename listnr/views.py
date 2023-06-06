from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from listnr.models import Task
from .tasks import fetch_comments
from .serializers import TaskSerializer

# Create your views here.
class TaskView(APIView):

    def post(self, request):
        data = request.data

        try:
            email = data['email']
            video_id = data['video_id']
            description = data['description']
        except KeyError:
            return Response({ "error": "invalid body parameters" }, status=status.HTTP_400_BAD_REQUEST)

        task = Task.objects.create(email=email, video_id=video_id, status='CREATED', description=description)
        
        celery_task = fetch_comments.delay(task.id)
        task.fetch_comments_id = celery_task.id
        task.save()

        return Response({ 'message': 'queued task' }, status=status.HTTP_200_OK)
    
    def get(self, request):
        data = request.data

        try:
            email = data['email']
        except KeyError:
            return Response({ "error": "invalid body parameters" }, status=status.HTTP_400_BAD_REQUEST)
        
        tasks = Task.objects.filter(email=email)

        if "status" in data:
            status = data['status']
            tasks = tasks.filter(status=status)
            
        
        serialized = TaskSerializer(tasks, many=True)

        return Response({ 'tasks': serialized.data })
    

class TaskDetailsView(APIView):

    def get(self, request, id):
        data = request.data

        email = data['email']
        
        try:
            task = Task.objects.get(email=email, id=id)
        except Exception as e:
            return Response({ 'error': 'could not get task' }, status=status.HTTP_400_BAD_REQUEST)
        serialized = TaskSerializer(task)

        return Response({ 'task': serialized.data })