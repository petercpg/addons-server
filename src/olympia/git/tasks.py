import olympia.core.logger

from olympia.amo.celery import task
from olympia.amo.decorators import use_primary_db
from olympia.versions.tasks import extract_version_to_git

from .models import GitExtractionEntry


log = olympia.core.logger.getLogger('z.git.task')


@task
@use_primary_db
def remove_git_extraction_entry(addon_pk):
    log.info(
        'Removing add-on "{}" from the git extraction queue.'.format(addon_pk)
    )
    GitExtractionEntry.objects.filter(
        addon_id=addon_pk, in_progress=True
    ).delete()


@task
@use_primary_db
def on_extraction_error(request, exc, traceback, addon_pk):
    log.error('Git extraction failed for add-on "{}".'.format(addon_pk))
    remove_git_extraction_entry(addon_pk)


@task
@use_primary_db
def extract_versions_to_git(addon_pk, version_pks):
    log.info(
        'Starting the git extraction of {} versions for add-on "{}".'.format(
            len(version_pks), addon_pk
        )
    )
    for version_pk in version_pks:
        extract_version_to_git(version_id=version_pk, force_extraction=True)
