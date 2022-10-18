from django.http import JsonResponse
from django.views.generic.list import BaseListView
from django.views.generic.detail import BaseDetailView
from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Q

from movies.models import Filmwork, PersonFilmwork


class MoviesApiMixin:
    model = Filmwork
    http_method_names = ['get']

    def get_queryset(self):
        roles = PersonFilmwork.Roles

        queryset = (
            self.model.objects
                .all()
                .values(
                    'id', 'title', 'description', 'creation_date',
                    'rating', 'type')
                .annotate(
                    genres=ArrayAgg(
                        'genres__name',
                        distinct=True,
                        filter=Q(genrefilmwork__pk__isnull=False)
                    ),
                    actors=ArrayAgg(
                        'persons__full_name',
                        distinct=True,
                        filter=Q(personfilmwork__role=roles.ACTOR)
                    ),
                    directors=ArrayAgg(
                        'persons__full_name',
                        distinct=True,
                        filter=Q(personfilmwork__role=roles.DIRECTOR)
                    ),
                    writers=ArrayAgg(
                        'persons__full_name',
                        distinct=True,
                        filter=Q(personfilmwork__role=roles.WRITER)
                    ))
        )
        return queryset

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)


class MoviesListApi(MoviesApiMixin, BaseListView):
    paginate_by = 50

    def get_context_data(self, *, object_list=None, **kwargs):
        queryset = self.get_queryset()

        paginator, page, queryset, _ = self.paginate_queryset(
            queryset,
            self.paginate_by
        )

        context = {
            'count': paginator.count,
            'total_pages': paginator.num_pages,
            'prev': (
                page.previous_page_number() if page.has_previous() else None
            ),
            'next': (
                page.next_page_number() if page.has_next() else None
            ),
            'results': list(queryset),
        }
        return context


class MoviesDetailApi(MoviesApiMixin, BaseDetailView):

    def get_context_data(self, *, object_list=None, **kwargs):
        pk = self.kwargs.get('pk')
        queryset = self.get_queryset().get(pk=pk)
        return queryset
