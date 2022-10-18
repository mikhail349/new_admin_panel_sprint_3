import uuid

from django.db import models
from django.utils.translation import gettext_lazy as _
from django.core.validators import MinValueValidator, MaxValueValidator


class UUIDMixin(models.Model):
    id = models.UUIDField(_('id'), primary_key=True, default=uuid.uuid4,
                          editable=False)

    class Meta:
        abstract = True


class TimeStampedMixin(models.Model):
    created_at = models.DateTimeField(_('created'), auto_now_add=True)
    updated_at = models.DateTimeField(_('modified'), auto_now=True)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True, null=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _('genre')
        verbose_name_plural = _('genres')
        constraints = [
            models.UniqueConstraint(
                fields=['name'],
                name='unique_name'
            ),
        ]


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('full name'), max_length=255)

    def __str__(self):
        return self.full_name

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('person')
        verbose_name_plural = _('persons')
        constraints = [
            models.UniqueConstraint(
                fields=['full_name'],
                name='unique_full_name'
            ),
        ]


class Filmwork(UUIDMixin, TimeStampedMixin):

    class Types(models.TextChoices):
        MOVIE = 'movie', _('movie')
        TV_SHOW = 'tv_show', _('tv show')

    title = models.CharField(_('title'), max_length=255)
    description = models.TextField(_('description'), blank=True, null=True)
    creation_date = models.DateField(_('creation date'), blank=True, null=True)
    rating = models.FloatField(_('rating'), blank=True, null=True,
                               validators=[MinValueValidator(0),
                                           MaxValueValidator(10)])
    type = models.CharField(_('type'), choices=Types.choices, max_length=255)
    file_path = models.FileField(_('file'), blank=True, null=True,
                                 upload_to='movies/')
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')
    persons = models.ManyToManyField(Person, through='PersonFilmwork')

    def __str__(self):
        return self.title

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('filmwork')
        verbose_name_plural = _('filmworks')
        indexes = [
            models.Index(fields=['type'], name='film_work_type_idx'),
        ]


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE,
                                  verbose_name=_('firmwork'))
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE,
                              verbose_name=_('genre'))
    created_at = models.DateTimeField(_('created'), auto_now_add=True)

    def __str__(self):
        return self.genre.name

    class Meta:
        db_table = "content\".\"genre_film_work"
        verbose_name = _('filmwork genre')
        verbose_name_plural = _('filmwork genres')
        constraints = [
            models.UniqueConstraint(
                fields=['film_work_id', 'genre_id'],
                name='unique_film_work_genre'
            ),
        ]


class PersonFilmwork(UUIDMixin):

    class Roles(models.TextChoices):
        ACTOR = 'actor', _('actor')
        PRODUCER = 'producer', _('producer')
        WRITER = 'writer', _('writer')
        DIRECTOR = 'director', _('director')

    person = models.ForeignKey(Person, on_delete=models.CASCADE,
                               verbose_name=_('person'))
    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE,
                                  verbose_name=_('filmwork'))
    role = models.CharField(_('role'), choices=Roles.choices, max_length=255)
    created_at = models.DateTimeField(_('created'), auto_now_add=True)

    def __str__(self):
        return self.person.full_name

    class Meta:
        db_table = "content\".\"person_film_work"
        verbose_name = _('filmwork person')
        verbose_name_plural = _('filmwork persons')
        constraints = [
            models.UniqueConstraint(
                fields=['film_work_id', 'person_id', 'role'],
                name='unique_film_work_person_role'
            ),
        ]
