# Generated by Django 2.2.7 on 2019-11-25 04:09

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('kanban', '0008_auto_20191125_1301'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='card_top30',
            name='column',
        ),
    ]
