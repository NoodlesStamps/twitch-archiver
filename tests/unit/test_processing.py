import unittest
from unittest.mock import patch, MagicMock, call
import logging
from pathlib import Path

from twitcharchiver.processing import Processing
from twitcharchiver.channel import Channel
# Stream is patched where it's used i.e. 'twitcharchiver.processing.Stream'
# from twitcharchiver.downloaders.stream import Stream # Not needed directly
from twitcharchiver.exceptions import TwitchAPIErrorForbidden
from twitcharchiver.vod import Vod
# ArchivedVod might need patching if convert_from_vod is hit by channel2's stream
# from twitcharchiver.vod import ArchivedVod


# Minimal conf for Processing instance
MIN_CONF = {
    "quiet": False, "chat": False, "video": True, "archive_only": False,
    "highlights": False, "live_only": False, "real_time_archiver": False,
    "unsorted": False, "config_dir": "mock_config_dir", "directory": "mock_output_dir",
    "discord_webhook": None, "pushbullet_key": None, "quality": "best",
    "threads": 1, "force_no_archive": False,
}

class TestProcessing(unittest.TestCase):

    @patch('twitcharchiver.processing.Database')  # Mock Database to avoid instantiation
    @patch('twitcharchiver.processing.signal.signal')  # Mock signal call in __init__
    @patch('twitcharchiver.processing.Path')      # Mock Path from pathlib
    @patch('twitcharchiver.processing.shutil')    # Mock shutil
    @patch('twitcharchiver.processing.Stream')    # Mock Stream class
    @patch('twitcharchiver.processing.ArchivedVod') # Mock ArchivedVod class
    def test_get_channel_skip_on_forbidden_error(self,
                                                 MockArchivedVod,
                                                 MockStream,
                                                 MockShutil,
                                                 MockPathLibPath, # Order matters for mock args
                                                 MockSignal,
                                                 MockDatabase):

        # Setup mock logger
        mock_logger = MagicMock(spec=logging.Logger)

        # Create Processing instance with a mock logger
        # We need to ensure that the logger used by Processing is our mock_logger
        # One way is to patch 'logging.getLogger' globally for this test or pass it if possible
        # Processing class gets logger via self.log = logging.getLogger()
        with patch('logging.getLogger', return_value=mock_logger):
            processing_instance = Processing(MIN_CONF)

        # Mock methods on the processing_instance that are not under test but get called
        processing_instance._start_download = MagicMock()
        processing_instance.vod_downloader = MagicMock()

        # Setup mock channels
        channel1 = MagicMock(spec=Channel)
        channel1.name = "ForbiddenChannel"
        channel1.id = "123"
        channel1.is_live = MagicMock(return_value=True)
        channel1.get_channel_archives = MagicMock(return_value=[])
        channel1.get_channel_highlights = MagicMock(return_value=[])
        # Ensure 'get_latest_video' is also mocked if live_only=True (though it's False here)
        channel1.get_latest_video = MagicMock(return_value=None)


        channel2 = MagicMock(spec=Channel)
        channel2.name = "GoodChannel"
        channel2.id = "456"
        channel2.is_live = MagicMock(return_value=True)
        channel2.get_channel_archives = MagicMock(return_value=[])
        channel2.get_channel_highlights = MagicMock(return_value=[])
        channel2.get_latest_video = MagicMock(return_value=None)

        # Configure MockStream side effect for its __init__
        # This function will be called instead of Stream.__init__
        mock_stream_channel2_instance = MagicMock()
        mock_stream_channel2_instance.vod = MagicMock(spec=Vod)
        mock_stream_channel2_instance.vod.v_id = "vod_for_channel2" # Give it a VOD ID
        mock_stream_channel2_instance.vod.duration = 600 # Some duration
        mock_stream_channel2_instance.has_ended = True # Simplifies logic for channel2
        mock_stream_channel2_instance.output_dir = MagicMock(spec=Path)

        # Prepare a mock response for TwitchAPIErrorForbidden
        mock_forbidden_response = MagicMock()
        mock_forbidden_response.status_code = 403
        mock_forbidden_response.url = "https://api.twitch.tv/helix/mock_forbidden_endpoint"
        mock_forbidden_response.text = '{"error":"Forbidden","status":403,"message":"Mocked API Forbidden Error"}'


        def stream_init_side_effect(channel_obj, vod_obj, output_dir, quality, quiet, force_no_archive_flag):
            if channel_obj.name == "ForbiddenChannel":
                raise TwitchAPIErrorForbidden(mock_forbidden_response)
            elif channel_obj.name == "GoodChannel":
                # Return the pre-configured mock instance for channel2
                # Ensure the returned mock has attributes accessed by Processing
                mock_stream_channel2_instance.vod.channel = channel_obj # Link channel back if needed
                return mock_stream_channel2_instance
            return MagicMock() # Default for any other unexpected calls

        MockStream.side_effect = stream_init_side_effect

        # Mock ArchivedVod.convert_from_vod if it's used by force_no_archive or other paths
        # For this test, with force_no_archive=False, and simple channel2 stream, it might not be hit.
        # If it were, it should return a mock ArchivedVod instance.
        MockArchivedVod.convert_from_vod = MagicMock(return_value=MagicMock(spec=Vod))


        # Action
        processing_instance.get_channel([channel1, channel2])

        # Assertions
        # 1. Check logs for channel1 skip message
        # Example: self.log.warning(f"Skipping channel {channel.name} due to a Forbidden error: {e}")
        found_log = False
        for log_call in mock_logger.warning.call_args_list:
            args, _ = log_call
            if len(args) > 0:
                log_message = args[0]
                if "Skipping channel ForbiddenChannel" in log_message and "Forbidden error" in log_message:
                    found_log = True
                    break
        self.assertTrue(found_log, "Log message for skipping ForbiddenChannel not found or incorrect.")

        # 2. Verify Stream.__init__ was called for channel1 (leading to the exception)
        #    and for channel2 (successful instantiation)
        #    The side_effect function is MockStream itself in this context of patching the class
        self.assertEqual(MockStream.call_count, 2, "Stream.__init__ should be called twice.")

        # Check calls to Stream constructor
        # Call arguments are (channel, Vod(), self.output_dir, self.quality, self.quiet, False)
        # We can check the channel argument specifically
        args_list = MockStream.call_args_list

        # Check that the first call was for channel1
        self.assertIs(args_list[0][0][0], channel1, "Stream not called with channel1 first")
        # Check that the second call was for channel2
        self.assertIs(args_list[1][0][0], channel2, "Stream not called with channel2 second")

        # 3. Verify that processing continued for channel2.
        #    One way is to check if channel2.is_live was called. (Already implicitly tested by Stream call)
        #    Or, if _start_download would have been called for channel2's stream if it was complex.
        #    In our simplified channel2 setup (has_ended=True, v_id present), _start_download
        #    for the *stream object* is not directly called in the initial live check.
        #    Instead, channel2's VODs (none in this setup) would go to vod_downloader.
        #    So, `processing_instance.vod_downloader` being called is a good sign.
        self.assertTrue(processing_instance.vod_downloader.called, "vod_downloader should be called for channel2's VOD queue")

        # Ensure _start_download was NOT called for channel1's stream
        # And also not for channel2's stream object directly due to has_ended=True
        # This means _start_download on processing_instance should not have been called for stream objects
        # (it could be called by vod_downloader for actual VOD objects, but vod_downloader is fully mocked here)
        # For this specific test, we are not deeply testing channel2's successful processing path,
        # only that channel1 was skipped and channel2 was attempted.

        # Verify that `get_channel_archives` was called for channel2, as it's part of its processing path
        # after the live stream check.
        channel2.get_channel_archives.assert_called_once()


if __name__ == '__main__':
    unittest.main()
