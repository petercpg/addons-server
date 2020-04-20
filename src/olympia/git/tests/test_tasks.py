from olympia.amo.tests import TestCase, addon_factory

from olympia.git.models import AddonGitExtraction
from olympia.git.tasks import remove_gitextraction_lock


class TestRemoveAddonGitExtractionLock(TestCase):
    def test_remove_lock(self):
        addon = addon_factory()
        AddonGitExtraction.objects.create(addon=addon, in_progress=True)

        assert addon.git_extraction_is_in_progress

        remove_gitextraction_lock(addon_pk=addon.pk)
        addon.refresh_from_db()

        assert not addon.git_extraction_is_in_progress

    def test_remove_does_not_create_a_gitextraction_object(self):
        addon = addon_factory()

        remove_gitextraction_lock(addon_pk=addon.pk)

        assert AddonGitExtraction.objects.count() == 0
