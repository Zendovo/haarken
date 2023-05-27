from django.db import models

# Create your models here.
class Task(models.Model):
    email = models.CharField("Email", max_length=255)
    video_id = models.CharField("Video ID", max_length=255)
    description = models.CharField("Description", max_length=2048)
    status = models.CharField("Status", max_length=50)
    all_comments_data = models.TextField("Dump of Comments Data", blank=True)
    analysed_comments = models.TextField("Dump of Analysed Comments", blank=True)
    fetch_comments_id = models.CharField("Celery Fetch Comments Task ID", max_length=255)
    analyse_comments_id = models.CharField("Celery Fetch Comments Task ID", max_length=255, blank=True)
    parse_analysis_id = models.CharField("Celery Fetch Comments Task ID", max_length=255, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)