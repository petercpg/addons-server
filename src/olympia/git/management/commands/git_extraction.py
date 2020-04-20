import waffle

from celery import chain
from django.conf import settings
from django.core.management.base import BaseCommand

import olympia.core.logger

from olympia.amo.decorators import use_primary_db
from olympia.files.utils import lock
from olympia.git.models import AddonGitExtraction, GitExtractionEntry
from olympia.git.tasks import remove_gitextraction_lock
from olympia.versions.models import Version
from olympia.versions.tasks import extract_version_to_git


log = olympia.core.logger.getLogger('z.git.git_extraction')

LOCK_NAME = 'git-extraction'  # Name of the lock() used.
SWITCH_NAME = 'enable-git-extraction-cron'  # Name of the waffle switch.


class Command(BaseCommand):
    help = 'Extract add-on versions to Git'

    @use_primary_db
    def handle(self, *args, **options):
        if not waffle.switch_is_active(SWITCH_NAME):
            log.info(
                'Not running git_extraction command because switch "{}" is '
                'not active.'.format(SWITCH_NAME)
            )
            return

        # Get a lock before doing anything, we don't want to have multiple
        # instances of the command running in parallel.
        with lock(settings.TMP_PATH, LOCK_NAME) as lock_attained:
            if not lock_attained:
                # We didn't get the lock...
                log.error('{} lock present, aborting.'.format(LOCK_NAME))
                return

            # If an add-on ID is present more than once, the `extract_addon()`
            # method will skip all but the first call because the add-on will
            # be locked for git extraction.
            entries = GitExtractionEntry.objects.order_by('created').all()
            for entry in entries:
                self.extract_addon(entry)

    def extract_addon(self, entry):
        """
        This method takes a GitExtractionEntry entry (which is bound to an
        add-on ID) and creates a chain of Celery tasks to extract each version
        in a git repository.
        """
        addon = entry.addon
        log.info('Starting extraction of add-on "{}".'.format(addon.pk))

        if addon.git_extraction_is_in_progress:
            log.info(
                'Aborting extraction of addon "{}" to git storage '
                'because it is already in progress.'.format(addon.pk)
            )
            return

        log.info('Locking add-on "{}" before extraction.'.format(addon.pk))
        addon_lock, created = AddonGitExtraction.objects.update_or_create(
            addon=addon, defaults={'in_progress': True}
        )

        try:
            # Retrieve all the versions to extract sorted by creation date.
            versions_to_extract = (
                Version.unfiltered.filter(addon=addon, git_hash='')
                .order_by('created')
                .values_list('pk', flat=True)
            )

            if len(versions_to_extract) == 0:
                log.info(
                    'No version to extract for add-on "{}", '
                    'exiting.'.format(addon.pk)
                )
                addon_lock.update(in_progress=False)
                return

            tasks = []
            for version_pk in versions_to_extract:
                tasks.append(
                    extract_version_to_git.si(
                        version_id=version_pk, force_extraction=True
                    ).on_error(remove_gitextraction_lock.si(addon.pk))
                )
            tasks.append(remove_gitextraction_lock.si(addon.pk))

            log.info(
                'Submitted {} tasks to extract add-on "{}".'.format(
                    len(tasks), addon.pk
                )
            )
            chain(*tasks).delay()
        finally:
            log.info(
                'Removing add-on "{}" from the git extraction '
                'queue.'.format(addon.pk)
            )
            entry.delete()
