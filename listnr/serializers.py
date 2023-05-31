from rest_framework import serializers


class TaskSerializer(serializers.Serializer):
    class Meta:
        fields = ["id", "email", "status", "video_id", "description", "final_results"]
