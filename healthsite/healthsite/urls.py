from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path('lookup/', include('lookup.urls')),
    path('admin/', admin.site.urls),
]