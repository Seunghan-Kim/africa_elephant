# Generated by Django 2.2.7 on 2019-11-22 05:11

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('kanban', '0004_card_slug_notnull'),
    ]

    operations = [
        migrations.AddField(
            model_name='column',
            name='date',
            field=models.CharField(default=django.utils.timezone.now, max_length=255),
            preserve_default=False,
        ),
    ]