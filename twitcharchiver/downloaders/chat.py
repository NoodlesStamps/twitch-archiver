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
    write_json_file,
    write_file_line_by_line,
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
        self.temp_chat_log_path = Path(self.output_dir, "verbose_chat.tmp.jsonl")

        # store chat log and seen message ids
        self._chat_log: list = []
        self._chat_message_ids: set = set()
        self.last_message_offset: int = 0

        # load chat from file if a download was attempted previously
        self.load_from_file()

    def export_metadata(self):
        write_json_file(self.vod.to_dict(), Path(self.output_dir, "vod.json"))

    def load_from_file(self):
        """
        Loads the chat log stored in the output directory.
        Priority is given to the temporary JSON Lines file if it exists.
        """
        if self.temp_chat_log_path.exists():
            self._log.debug("Loading chat log from temporary JSON Lines file.")
            loaded_messages = []
            try:
                with open(self.temp_chat_log_path, "r", encoding="utf8") as tmp_chat_file:
                    for line in tmp_chat_file:
                        try:
                            msg = json.loads(line)
                            loaded_messages.append(msg)
                            self._chat_message_ids.add(msg["id"])
                        except json.JSONDecodeError:
                            self._log.warning(f"Skipping invalid JSON line in {self.temp_chat_log_path}: {line.strip()}")

                # It's generally safer to extend if _chat_log might have prior entries,
                # though for a fresh load, direct assignment is also fine.
                # We also need to ensure messages are unique if this method is called multiple times.
                # Current logic elsewhere re-initializes _chat_log and _chat_message_ids or relies on _chat_message_ids for uniqueness.
                # For simplicity, let's assume this is the primary loading mechanism at init.
                self._chat_log = loaded_messages
                self._log.debug(f"Loaded {len(self._chat_log)} messages from temporary file.")
                # Schema check might be less critical here if we assume .tmp.jsonl is always new format
                # However, if we want to be extremely cautious:
                if self._chat_log and "contentOffsetSeconds" not in self._chat_log[0].keys():
                    self._log.warning(
                        "Temporary chat log has incompatible schema. Clearing loaded messages."
                    )
                    self._chat_log = []
                    self._chat_message_ids = set()
                    # Optionally, delete or rename the problematic tmp file here

                if self._chat_log:
                    self.last_message_offset = self._chat_log[-1]["contentOffsetSeconds"]
                # Clear _chat_log as its contents are from the temp file and already persisted
                self._chat_log = []
                return
            except Exception as e:
                self._log.error(f"Error loading from {self.temp_chat_log_path}: {e}. Falling back to verbose_chat.json if possible.")
                # Clear any partially loaded data
                self._chat_log = []
                self._chat_message_ids = set()

        # If temp file doesn't exist or failed to load, try the original verbose_chat.json
        try:
            legacy_chat_path = Path(self.output_dir, "verbose_chat.json")
            if not legacy_chat_path.exists():
                self._log.debug("No chat log file found to load.")
                return

            with open(legacy_chat_path, "r", encoding="utf8") as chat_file:
                self._log.debug("Loading chat log from verbose_chat.json.")
                chat_log = json.loads(chat_file.read())

            # ignore chat logs created with older incompatible schema - see v2.2.1 changes
            if chat_log and "contentOffsetSeconds" not in chat_log[0].keys():
                self._log.debug(
                    "Ignoring chat log loaded from verbose_chat.json as it is incompatible."
                )
                return # Do not load incompatible schema

            self._log.debug("Chat log found for VOD %s from verbose_chat.json.", self.vod)
            # Merge with any existing messages if any (e.g. from a failed .tmp.jsonl load)
            # though typically _chat_log would be empty here unless specifically designed otherwise.
            # For now, assume direct load, but be mindful of _chat_message_ids to avoid duplicates.
            for msg in chat_log:
                if msg["id"] not in self._chat_message_ids:
                    self._chat_log.append(msg)
                    self._chat_message_ids.add(msg["id"])

            if self._chat_log: # If loaded from verbose_chat.json and it had content
                self.last_message_offset = self._chat_log[-1]["contentOffsetSeconds"]
            # Do not clear self._chat_log here; _download will process it.
            return

        except FileNotFoundError:
            # This specific exception was for the legacy path, already handled by legacy_chat_path.exists()
            # General exceptions during loading are caught above.
            self._log.debug("verbose_chat.json not found.")
            return
        except json.JSONDecodeError:
            self._log.error(f"Error decoding JSON from {legacy_chat_path}.")
            return
        except Exception as e:
            self._log.error(f"Unexpected error loading from {legacy_chat_path}: {e}")
            return

    def start(self):
        """
        Downloads the chat for the given VOD and exports both a readable and JSON-formatted log to the provided
        directory.

        :return: list of dictionaries containing chat message data
        :rtype: list[dict]
        """
        try:
            # create output dir
            Path(self.output_dir).mkdir(parents=True, exist_ok=True)

            self._download()
            self.export_chat_logs()

            # use while loop for archiving live VODs
            while self.vod.is_live():
                self._log.debug(
                    "VOD is still live, attempting to download new chat segments."
                )
                _start_timestamp: float = datetime.now(timezone.utc).timestamp()

                # refresh VOD metadata
                self.vod.refresh_vod_metadata()

                # begin downloader from offset of previous log
                self._download(offset=self.last_message_offset)
                self.export_chat_logs()

                # sleep if processing time < CHECK_INTERVAL before fetching new messages
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

                # refresh VOD metadata
                self.vod.refresh_vod_metadata()
                self._download(offset=self.last_message_offset)

        except (TwitchAPIErrorNotFound, TwitchAPIErrorForbidden):
            self._log.debug(
                "Error 403 or 404 returned when checking for new chat segments - VOD was likely deleted."
            )

        finally:
            self.export_chat_logs()
            # Fallback cleanup for the temporary file
            if hasattr(self, 'temp_chat_log_path') and self.temp_chat_log_path.exists():
                try:
                    self._log.debug(f"Cleaning up temporary chat log file in start() finally: {self.temp_chat_log_path}")
                    self.temp_chat_log_path.unlink()
                except OSError as e:
                    self._log.error(f"Error deleting temporary chat log file {self.temp_chat_log_path} in start() finally: {e}")

        # logging
        self._log.debug("Found %s chat messages.", len(self._chat_log))

        # set archival flag if ArchivedVod provided
        if isinstance(self.vod, ArchivedVod):
            self.vod.chat_archived = True

        self._log.info("Finished archiving VOD chat.")

    def _download(self, offset: int = 0):
        """
        Downloads the chat log in its entirety.

        :param offset: time to begin archiving from in seconds
        :return: list of all chat messages
        :rtype: list
        """
        _progress = Progress()
        # start_len = len(self._chat_log) # _chat_log will be cleared or empty

        # Process messages loaded from legacy verbose_chat.json first
        if self._chat_log:
            self._log.debug(f"Processing {len(self._chat_log)} messages pre-loaded in self._chat_log.")
            with open(self.temp_chat_log_path, "a", encoding="utf8") as tmp_chat_file:
                for m_legacy in self._chat_log:
                    # Assuming these messages, if loaded, already populated _chat_message_ids
                    # and their offset was recorded in self.last_message_offset.
                    # We just need to ensure they are in the temp file.
                    # No, load_from_file already added them to _chat_message_ids.
                    # We write them here to ensure they are in the .tmp.jsonl
                    tmp_chat_file.write(json.dumps(m_legacy) + "\n")
            self._chat_log = [] # Clear after writing to temp file

        messages_retrieved_count = 0

        # grab initial chat segment containing cursor
        _initial_segment, _cursor = self._get_chat_segment(offset=offset)
        with open(self.temp_chat_log_path, "a", encoding="utf8") as tmp_chat_file:
            for m in _initial_segment:
                if m["id"] not in self._chat_message_ids:
                    self._chat_message_ids.add(m["id"])
                    tmp_chat_file.write(json.dumps(m) + "\n")
                    self.last_message_offset = m["contentOffsetSeconds"]
                    messages_retrieved_count +=1
                    # DO NOT APPEND TO self._chat_log

        while True:
            if not _cursor:
                self._log.debug(
                    f"{messages_retrieved_count} new messages retrieved from Twitch this run."
                )
                break

            self._log.debug("Fetching chat segments at cursor: %s.", _cursor)
            # grab next chat segment along with cursor for next segment
            _segment, _cursor = self._get_chat_segment(cursor=_cursor)
            segment_message_count_this_fetch = 0
            with open(self.temp_chat_log_path, "a", encoding="utf8") as tmp_chat_file:
                for m in _segment:
                    if m["id"] not in self._chat_message_ids:
                        self._chat_message_ids.add(m["id"])
                        tmp_chat_file.write(json.dumps(m) + "\n")
                        self.last_message_offset = m["contentOffsetSeconds"]
                        messages_retrieved_count += 1
                        segment_message_count_this_fetch +=1
                        # DO NOT APPEND TO self._chat_log

            if not self._quiet and _segment: # Ensure _segment is not empty and we are not in quiet mode
                # Use the offset of the last message in the current segment for progress
                # This assumes _segment is a list of messages and each message is a dict with "contentOffsetSeconds"
                last_msg_in_segment_offset = _segment[-1]["contentOffsetSeconds"]
                _progress.print_progress(
                    int(last_msg_in_segment_offset), self.vod.duration
                )
            elif not _segment and _cursor: # Edge case: received a cursor but no messages
                self._log.debug("Received a cursor but no messages in the segment, or segment was empty.")

            # vod duration in seconds is used as the total for progress bar
            # comment offset is used to track what's been done
            # could be done properly if there was a way to get the total number of comments
            if not self._quiet:
                _progress.print_progress(
                    int(_segment[-1]["contentOffsetSeconds"]), self.vod.duration
                )

    def _get_chat_segment(self, offset: int = 0, cursor: str = ""):
        """
        Retrieves a chat segment and any subsequent segments from a given offset.

        :param offset: offset in seconds to begin retrieval from
        :type offset: int
        :param cursor: cursor returned by a previous call of this function
        :type cursor: str
        :returns: list of comments, cursor if one is returned from twitch
        :rtype: list, str
        """
        # build payload
        if offset != 0:
            _p = [
                {
                    "operationName": "VideoCommentsByOffsetOrCursor",
                    "variables": {
                        "videoID": str(self.vod.v_id),
                        "contentOffsetSeconds": offset,
                    },
                }
            ]

        else:
            _p = [
                {
                    "operationName": "VideoCommentsByOffsetOrCursor",
                    "variables": {"videoID": str(self.vod.v_id), "cursor": cursor},
                }
            ]

        _p[0]["extensions"] = {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "b70a3591ff0f4e0313d126c6a1502d79a1c02baebb288227c582044aa76adf6a",
            }
        }

        _r = self._api.post_request("https://gql.twitch.tv/gql", j=_p).json()
        _comments = _r[0]["data"]["video"]["comments"]

        # check if next page exists
        if _comments:
            if _comments["pageInfo"]["hasNextPage"]:
                return [c["node"] for c in _comments["edges"]], _comments["edges"][-1][
                    "cursor"
                ]

            return [c["node"] for c in _comments["edges"]], None

        return [], None

    def generate_readable_chat_log(self, chat_log: list):
        """
        Converts the raw chat log into a human-readable format.

        :param chat_log: list of messages to generate log from
        :type chat_log: list
        """
        _r_chat_log = []
        for _comment in chat_log:
            _created_time = parse_twitch_timestamp(_comment["createdAt"])

            _comment_time = (
                f"{get_time_difference(self.vod.created_at, _created_time):.3f}"
            )

            # catch comments without commenter information
            if _comment["commenter"]:
                _user_name = str(_comment["commenter"]["displayName"])
            else:
                _user_name = "~MISSING_COMMENTER_INFO~"

            # catch comments without data
            if _comment["message"]["fragments"]:
                _user_message = str(_comment["message"]["fragments"][0]["text"])
            else:
                _user_message = "~MISSING_MESSAGE_INFO~"

            _user_badges = ""
            try:
                for _badge in _comment["message"]["userBadges"]:
                    if "broadcaster" in _badge["setID"]:
                        _user_badges += "(B)"

                    if "moderator" in _badge["setID"]:
                        _user_badges += "(M)"

                    if "subscriber" in _badge["setID"]:
                        _user_badges += "(S)"

            except KeyError:
                pass

            # FORMAT: [TIME] (B1)(B2)NAME: MESSAGE
            _r_chat_log.append(
                f"[{_comment_time}] {_user_badges}{_user_name}: {_user_message}"
            )

        return _r_chat_log

    def export_chat_logs(self):
        """
        Exports a readable and a JSON-formatted chat log to the output directory.
        Prioritizes using the temporary chat log file if it exists and is not empty.
        """
        source_chat_log = self._chat_log
        log_source_description = "in-memory _chat_log"

        if self.temp_chat_log_path.exists() and os.path.getsize(self.temp_chat_log_path) > 0:
            self._log.debug(f"Exporting chat logs from {self.temp_chat_log_path}")
            messages_from_temp_file = []
            try:
                with open(self.temp_chat_log_path, "r", encoding="utf8") as tmp_chat_file:
                    for line in tmp_chat_file:
                        try:
                            messages_from_temp_file.append(json.loads(line))
                        except json.JSONDecodeError:
                            self._log.warning(f"Skipping invalid JSON line in {self.temp_chat_log_path} during export: {line.strip()}")
                source_chat_log = messages_from_temp_file
                log_source_description = f"{self.temp_chat_log_path}"
            except Exception as e:
                self._log.error(f"Error reading from {self.temp_chat_log_path} during export: {e}. Defaulting to in-memory _chat_log.")
                # Fallback to _chat_log if temp file reading fails catastrophically
                source_chat_log = self._chat_log
                log_source_description = "in-memory _chat_log (fallback)"
        else:
            self._log.debug("Exporting chat logs from in-memory _chat_log (temp log not found or empty).")

        if not source_chat_log:
            self._log.info(f"No chat messages found in {log_source_description} to export.")
            # Ensure empty files are written if there's no data, to reflect an empty chat.
            # Or, decide if an error or different handling is needed.
            # Current utils.write_* functions handle empty lists correctly (write empty file/list).

        write_file_line_by_line(
            self.generate_readable_chat_log(source_chat_log),
            Path(self.output_dir, "readable_chat.txt"),
        )
        write_json_file(source_chat_log, Path(self.output_dir, "verbose_chat.json"))
        self._log.debug(f"Chat logs exported using data from {log_source_description}.")

        # If export was successful and source was the temp file, delete the temp file.
        if log_source_description == f"{self.temp_chat_log_path}" and self.temp_chat_log_path.exists():
            try:
                self._log.debug(f"Deleting temporary chat log file: {self.temp_chat_log_path}")
                self.temp_chat_log_path.unlink()
            except OSError as e:
                self._log.error(f"Error deleting temporary chat log file {self.temp_chat_log_path}: {e}")

    def get_message_count(self):
        """
        Fetches the total number of unique chat messages processed.

        :return:
        """
        return len(self._chat_message_ids)
