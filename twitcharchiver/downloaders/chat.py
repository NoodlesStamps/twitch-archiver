"""
Module used for downloading chat logs for a given Twitch VOD.
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from time import sleep

from twitcharchiver.api import Api
from twitcharchiver.downloader import Downloader
from twitcharchiver.exceptions import (
    TwitchAPIErrorNotFound,
    TwitchAPIErrorForbidden,
)
from twitcharchiver.utils import (
    Progress,
    get_time_difference,
    # write_json_file, # Replaced with incremental writing
    # write_file_line_by_line, # Replaced with incremental writing
    build_output_dir_name,
    time_since_date,
    parse_twitch_timestamp,
)
from twitcharchiver.vod import Vod, ArchivedVod

CHECK_INTERVAL = 60


class Chat(Downloader):
    """
    Class which handles the downloading, updating and importing of chat logs and the subsequent fomatting and archival.
    """

    def __init__(
        self, vod: Vod, parent_dir: Path = Path(os.getcwd()), quiet: bool = False
    ):
        """
        Initialize class variables.

        :param vod: VOD to be downloaded
        :param parent_dir: path to parent directory for downloaded files
        :param quiet: boolean whether to print progress
        """
        # init downloader
        super().__init__(parent_dir, quiet)

        # setup api with required header
        self._api = Api()
        self._api.add_headers({"Client-Id": "ue6666qo983tsx6so1t0vnawi233wa"})

        # vod-specific vars
        self.vod: Vod = vod
        # create output dir
        self.output_dir = Path(
            self._parent_dir,
            build_output_dir_name(self.vod.title, self.vod.created_at, self.vod.v_id),
        )

        # store seen message ids and total message count
        self._chat_message_ids: set = set()
        self._total_messages_processed: int = 0
        self._last_message_offset: float = 0.0


        # Paths for chat files
        self._verbose_chat_file_path = Path(self.output_dir, "verbose_chat.jsonl")
        self._readable_chat_file_path = Path(self.output_dir, "readable_chat.txt")

        # load chat from file if a download was attempted previously
        self._load_state_from_file()

    def export_metadata(self):
        # Utility function to write JSON, can be kept or moved to utils if generalized
        def _write_json_file(data, path):
            with open(path, "w", encoding="utf8") as f:
                json.dump(data, f, indent=4)
        _write_json_file(self.vod.to_dict(), Path(self.output_dir, "vod.json"))

    def _load_state_from_file(self):
        """
        Loads the last message offset and seen message IDs from the existing chat log file.
        Assumes verbose_chat.jsonl contains one JSON object per line.
        """
        if not self._verbose_chat_file_path.exists():
            return

        self._log.debug("Loading chat log from file: %s", self._verbose_chat_file_path)
        try:
            with open(self._verbose_chat_file_path, "r", encoding="utf8") as chat_file:
                for line in chat_file:
                    if line.strip():
                        try:
                            msg = json.loads(line)
                            self._chat_message_ids.add(msg["id"])
                            self._last_message_offset = msg.get("contentOffsetSeconds", self._last_message_offset)
                            self._total_messages_processed += 1
                        except json.JSONDecodeError:
                            self._log.warning("Skipping malformed line in chat log: %s", line.strip())
            self._log.debug(
                "Resuming from offset %s, found %s existing messages.",
                self._last_message_offset,
                self._total_messages_processed
            )
        except Exception as e:
            self._log.error("Error loading chat log state from file: %s", e)
            # Reset state if loading fails catastrophically
            self._chat_message_ids = set()
            self._total_messages_processed = 0
            self._last_message_offset = 0.0


    def start(self):
        """
        Downloads the chat for the given VOD and exports both a readable and JSON-formatted log to the provided
        directory incrementally.
        """
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

        # Open files once and pass file handlers
        try:
            with open(self._verbose_chat_file_path, "a", encoding="utf8") as verbose_fh, \
                 open(self._readable_chat_file_path, "a", encoding="utf8") as readable_fh:

                self._download(offset=self._last_message_offset, verbose_fh=verbose_fh, readable_fh=readable_fh)

                # use while loop for archiving live VODs
                while self.vod.is_live():
                    self._log.debug(
                        "VOD is still live, attempting to download new chat segments."
                    )
                    _start_timestamp: float = datetime.now(timezone.utc).timestamp()

                    # refresh VOD metadata
                    self.vod.refresh_vod_metadata()

                    # Determine offset for next download
                    # self._last_message_offset should be updated by _download
                    self._download(offset=self._last_message_offset, verbose_fh=verbose_fh, readable_fh=readable_fh)

                    _loop_time = int(
                        datetime.now(timezone.utc).timestamp() - _start_timestamp
                    )
                    if _loop_time < CHECK_INTERVAL:
                        sleep(CHECK_INTERVAL - _loop_time)

                # delay final archive pass if stream just ended
                self.vod.refresh_vod_metadata()
                if time_since_date(self.vod.created_at + self.vod.duration) < 300:
                    self._log.debug(
                        "Stream ended less than 5m ago, delaying before final chat archive attempt."
                    )
                    sleep(300)
                    self.vod.refresh_vod_metadata()
                    self._download(offset=self._last_message_offset, verbose_fh=verbose_fh, readable_fh=readable_fh)

        except (TwitchAPIErrorNotFound, TwitchAPIErrorForbidden):
            self._log.debug(
                "Error 403 or 404 returned when checking for new chat segments - VOD was likely deleted."
            )
        except Exception as e:
            self._log.error("An unexpected error occurred during chat download: %s", e, exc_info=True)
        finally:
            # Files are closed by with statement
            pass

        # logging
        self._log.debug("Processed %s chat messages in total.", self._total_messages_processed)

        # set archival flag if ArchivedVod provided
        if isinstance(self.vod, ArchivedVod):
            self.vod.chat_archived = True

        self._log.info("Finished archiving VOD chat.")

    def _write_segment_to_files(self, segment_messages: list, verbose_fh, readable_fh):
        """
        Writes a segment of messages to the verbose (JSONL) and readable text files.
        Updates self._last_message_offset and self._total_messages_processed.
        """
        if not segment_messages:
            return

        readable_log_segment = self._generate_readable_chat_log_segment(segment_messages)
        for msg_json, msg_readable in zip(segment_messages, readable_log_segment):
            verbose_fh.write(json.dumps(msg_json) + "\n")
            readable_fh.write(msg_readable + "\n")

        verbose_fh.flush()
        readable_fh.flush()

        self._last_message_offset = segment_messages[-1]["contentOffsetSeconds"]
        self._total_messages_processed += len(segment_messages)


    def _download(self, offset: float = 0, verbose_fh=None, readable_fh=None):
        """
        Downloads chat messages incrementally and writes them to the provided file handlers.

        :param offset: time to begin archiving from in seconds
        :param verbose_fh: file handler for the verbose JSONL chat log
        :param readable_fh: file handler for the readable text chat log
        """
        _progress = Progress()
        messages_in_this_run = 0

        # grab initial chat segment containing cursor
        current_segment_messages, _cursor = self._get_chat_segment(offset=offset)

        # Filter out already processed messages
        new_messages = [m for m in current_segment_messages if m["id"] not in self._chat_message_ids]
        if new_messages:
            self._write_segment_to_files(new_messages, verbose_fh, readable_fh)
            self._chat_message_ids.update(m["id"] for m in new_messages)
            messages_in_this_run += len(new_messages)

        while _cursor:
            self._log.debug("Fetching chat segments at cursor: %s.", _cursor)
            current_segment_messages, _cursor = self._get_chat_segment(cursor=_cursor)

            new_messages = [m for m in current_segment_messages if m["id"] not in self._chat_message_ids]
            if new_messages:
                self._write_segment_to_files(new_messages, verbose_fh, readable_fh)
                self._chat_message_ids.update(m["id"] for m in new_messages)
                messages_in_this_run += len(new_messages)

                if not self._quiet and new_messages:
                    _progress.print_progress(
                        int(new_messages[-1]["contentOffsetSeconds"]), self.vod.duration
                    )
            # If the segment had messages, but all were duplicates, _cursor might still be valid.
            # If current_segment_messages is empty, _cursor will be None, loop terminates.
            if not current_segment_messages and _cursor:
                self._log.debug("Received a cursor but no messages, ending segment processing for this cycle.")
                break


        if messages_in_this_run > 0:
            self._log.debug(f"{messages_in_this_run} new messages retrieved and written.")
        else:
            self._log.debug("No new messages retrieved in this run.")


    def _get_chat_segment(self, offset: float = 0, cursor: str = ""):
        """
        Retrieves a chat segment and any subsequent segments from a given offset.

        :param offset: offset in seconds to begin retrieval from. If 0 and no cursor, fetches from start.
        :type offset: float
        :param cursor: cursor returned by a previous call of this function
        :type cursor: str
        :returns: list of comments, cursor if one is returned from twitch
        :rtype: list, str
        """
        # build payload
        variables = {"videoID": str(self.vod.v_id)}
        if cursor:
            variables["cursor"] = cursor
        elif offset > 0: # Only use contentOffsetSeconds if it's greater than 0 and no cursor
            variables["contentOffsetSeconds"] = offset
        # If offset is 0 and no cursor, it implies fetching from the very beginning.
        # The API handles "contentOffsetSeconds": 0 or omitting it for the start.
        # To be explicit for resuming, we use it if > 0.

        _p = [
            {
                "operationName": "VideoCommentsByOffsetOrCursor",
                "variables": variables,
                "extensions": {
                    "persistedQuery": {
                        "version": 1,
                        "sha256Hash": "b70a3591ff0f4e0313d126c6a1502d79a1c02baebb288227c582044aa76adf6a",
                    }
                }
            }
        ]

        try:
            response = self._api.post_request("https://gql.twitch.tv/gql", j=_p)
            response.raise_for_status()  # Raise an exception for HTTP errors
            _r_json = response.json()
        except Exception as e:
            self._log.error("Failed to fetch chat segment: %s", e)
            return [], None # Return empty segment and no cursor on error

        if not _r_json or not isinstance(_r_json, list) or not _r_json[0].get("data"):
            self._log.warning("Received unexpected JSON structure from Twitch GQL API for chat.")
            return [], None

        _video_data = _r_json[0]["data"].get("video")
        if not _video_data or "comments" not in _video_data:
            self._log.debug("No 'comments' field in video data, or video data missing.")
            return [], None # VOD might have no comments or API response changed

        _comments = _video_data["comments"]

        edges = _comments.get("edges", [])
        messages = [c["node"] for c in edges if c and "node" in c] # Ensure node exists

        # check if next page exists
        if _comments.get("pageInfo", {}).get("hasNextPage") and edges and "cursor" in edges[-1]:
            return messages, edges[-1]["cursor"]

        return messages, None


    def _generate_readable_chat_log_segment(self, current_chat_segment: list):
        """
        Converts a segment of raw chat messages into a human-readable format.

        :param current_chat_segment: list of messages in the current segment
        :type current_chat_segment: list
        :return: list of readable chat messages for the segment
        :rtype: list[str]
        """
        _r_chat_log_segment = []
        if not current_chat_segment:
            return _r_chat_log_segment

        for _comment in current_chat_segment:
            _created_time = parse_twitch_timestamp(_comment["createdAt"])
            _comment_time = (
                f"{get_time_difference(self.vod.created_at, _created_time):.3f}"
            )

            if _comment.get("commenter"):
                _user_name = str(_comment["commenter"]["displayName"])
            else:
                _user_name = "~MISSING_COMMENTER_INFO~"

            _user_message = "~MISSING_MESSAGE_INFO~"
            if _comment.get("message", {}).get("fragments"):
                 # Concatenate all fragments to get the full message
                _user_message = "".join(fragment.get("text", "") for fragment in _comment["message"]["fragments"])


            _user_badges = ""
            try:
                for _badge in _comment.get("message", {}).get("userBadges", []):
                    if "broadcaster" in _badge.get("setID", ""):
                        _user_badges += "(B)"
                    if "moderator" in _badge.get("setID", ""):
                        _user_badges += "(M)"
                    if "subscriber" in _badge.get("setID", ""):
                        _user_badges += "(S)"
            except KeyError: # Should be less likely with .get()
                pass

            _r_chat_log_segment.append(
                f"[{_comment_time}] {_user_badges}{_user_name}: {_user_message}"
            )
        return _r_chat_log_segment

    # export_chat_logs is no longer needed as writing is incremental.
    # Keeping get_message_count as it might be useful for external checks.
    def get_message_count(self):
        """
        Fetches the total number of processed chat messages for this VOD.
        """
        return self._total_messages_processed
