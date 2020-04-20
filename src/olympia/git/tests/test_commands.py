from unittest import mock

from olympia.amo.tests import TestCase, create_switch, addon_factory
from olympia.git.models import GitExtractionEntry

from olympia.git.management.commands.git_extraction import (
    SWITCH_NAME,
    Command as GitExtractionCommand,
)


class TestGitExtraction(TestCase):
    def setUp(self):
        super().setUp()

        self.command = GitExtractionCommand()

    @mock.patch('olympia.git.management.commands.git_extraction.lock')
    def test_handle_does_not_run_if_switch_is_not_active(self, lock_mock):
        create_switch(SWITCH_NAME, active=False)

        self.command.handle()

        assert not lock_mock.called

    @mock.patch('olympia.git.management.commands.git_extraction.lock')
    def test_handle_tries_to_acquire_lock(self, lock_mock):
        create_switch(SWITCH_NAME, active=True)

        self.command.handle()

        assert lock_mock.called

    def test_handle_calls_extract_addon_for_each_addon_in_queue(self):
        create_switch(SWITCH_NAME, active=True)
        addon = addon_factory()
        GitExtractionEntry.objects.create(addon=addon)
        # Create a duplicate add-on.
        GitExtractionEntry.objects.create(addon=addon)
        # Create another add-on.
        GitExtractionEntry.objects.create(addon=addon_factory())
        self.command.extract_addon = mock.Mock()

        self.command.handle()

        assert self.command.extract_addon.call_count == 3
