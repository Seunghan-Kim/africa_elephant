# Generated by Django 2.2.7 on 2019-12-04 12:28

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('kanban', '0009_remove_card_top30_column'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='card_top30',
            options={'ordering': ['-drate']},
        ),
        migrations.RenameField(
            model_name='card_top30',
            old_name='rate',
            new_name='drate',
        ),
        migrations.AddField(
            model_name='card_top30',
            name='capital',
            field=models.FloatField(default=0),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='card_top30',
            name='mrate',
            field=models.FloatField(default=0),
            preserve_default=False,
        ),
    ]
