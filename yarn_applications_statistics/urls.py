from django.conf.urls import patterns, include, url

from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'yarn_usage_record.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),
    url(r'^$', 'data_collection.views.index', name='index'),
    url(r'^api/dataCollection/$', 'data_collection.views.api_data_collection', name='api_data_collection'),
    
   # url(r'^admin/', include(admin.site.urls)),
)
