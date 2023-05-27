from django.db import models

# Create your models here.
class Task(models.Model):
    email = models.CharField("Email", max_length=255)
    video_id = models.CharField("Video ID", max_length=255)
    status = models.CharField("Status", max_length=50)
    all_comments_data = models.TextField("JSON Dump of Comments Data")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)