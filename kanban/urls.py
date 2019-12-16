from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^new-card/$', views.new_card),
    url(r'^new-list/$', views.new_list),
    url(r'^new-col-name/$', views.new_col_name),
    url(r'^cards/(?P<card_id>\d+)/(?P<card_slug>[\w-]+)/$', views.view_card),
    url(r'^drop/$', views.drop),
    url(r'^$', views.index),
    url(r'^delete-column/$', views.delete_column),
    url(r'^new-card-name/$', views.new_card_name),
    url(r'^Top30/$', views.top30),
]
