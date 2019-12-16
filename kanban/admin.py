from django.contrib import admin

from .models import Board, Card, Column, Column_top30

admin.site.register(Board)
admin.site.register(Column)
admin.site.register(Card)
admin.site.register(Column_top30)
