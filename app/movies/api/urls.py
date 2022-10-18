from django.urls import path
from django.conf.urls import include

import movies.api.v1.urls as movies_api_v1_urls


urlpatterns = [
    path('v1/', include(movies_api_v1_urls))
]
