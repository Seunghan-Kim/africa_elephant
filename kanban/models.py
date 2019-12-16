from django.db import models
from django.utils.text import slugify


class Board(models.Model):
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name


class Column(models.Model):
    board = models.ForeignKey('Board', related_name='columns', on_delete=models.CASCADE)
    title = models.CharField(max_length=255)
    

    class Meta:
        ordering = ['id']

    def __str__(self):
        return '{} - {}'.format(self.board.name, self.title)

class Column_top30(models.Model):
    board = models.ForeignKey('Board', related_name='columns_top30', on_delete=models.CASCADE)
    title = models.CharField(max_length=255)
    date = models.CharField(max_length=255)

    class Meta:
        ordering = ['id']

    def __str__(self):
        return '{} - {} - {}'.format(self.board.name, self.title, self.date)


class Card(models.Model):
    column = models.ForeignKey('Column', related_name='cards', on_delete=models.CASCADE)
    title = models.CharField(max_length=255)
    slug = models.SlugField(allow_unicode=True)

    class Meta:
        ordering = ['id']

    def __str__(self):
        return '{} - {} - {}'.format(self.column.board.name, self.column.title, self.title)

    def save(self, *args, **kwargs):
        if not self.slug:
            self.slug = slugify(self.title, allow_unicode=True)
        return super().save(*args, **kwargs)

class Card_top30(models.Model):
    # column = models.ForeignKey('Board', related_name='cards_top30', on_delete=models.CASCADE)
    title = models.CharField(max_length=30)
    drate = models.FloatField()
    market = models.CharField(max_length=5)
    mrate = models.FloatField()
    capital = models.FloatField()
    code = models.CharField(max_length=10)

    slug = models.SlugField(allow_unicode=True)

    class Meta:
        ordering = ['-drate']

    def __str__(self):
        return '{} - {} - {}'.format(self.column.board.name, self.column.title, self.title)

    def save(self, *args, **kwargs):
        if not self.slug:
            self.slug = slugify(self.title, allow_unicode=True)
        return super().save(*args, **kwargs)

class Date_check(models.Model):
    date = models.CharField(max_length=255)

    class Meta:
        ordering = ['id']

    def __str__(self):
        return '{}'.format(self.date)