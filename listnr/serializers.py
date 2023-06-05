from rest_framework import serializers
from listnr.models import Task


class TaskSerializer(serializers.ModelSerializer):
    class Meta:
        model = Task
        fields = ["id", "email", "status", "video_id", "description", "final_results"]
