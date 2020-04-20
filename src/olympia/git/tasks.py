import olympia.core.logger

from olympia.amo.celery import task
from olympia.amo.decorators import use_primary_db

from .models import GitExtraction


log = olympia.core.logger.getLogger('z.git.task')


@task
@use_primary_db
def remove_gitextraction_lock(addon_pk):
    log.info('Removing GitExtraction lock for add-on "{}".'.format(addon_pk))
    GitExtraction.objects.filter(addon_id=addon_pk).update(in_progress=False)
