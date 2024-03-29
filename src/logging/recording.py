# TODO: Add Ability To Edit Files In Place
# https://github.com/asciinema/asciinema/blob/develop/doc/asciicast-v2.md
# The extension should be .cast and the mime type should be application/x-asciicast

# TODO: Read Version 1 STDOUT
# TODO: Write Everything

import os
import json

from io import TextIOWrapper, StringIO
from typing import Generator, Union, Optional, Any


class Theme:
    foreground: Optional[str]  # Normal Text Color
    background: Optional[str]  # Normal Background Color
    palette: Optional[str]  # List of 8 or 16 Colors, Separated By Colons (In CSS Hex Codes #rrggbb)

    def __init__(self, theme: Optional[dict] = None) -> None:
        if theme is not None:
            self.set_theme(theme=theme)

    def set_theme(self, theme: dict) -> None:
        """
            Save Theme To Class
        """
        self.foreground = theme["foreground"] if "foreground" in theme else None
        self.background = theme["background"] if "background" in theme else None
        self.palette = theme["palette"] if "palette" in theme else None

    def get_theme(self) -> dict:
        """
            Recreate Theme As Dictionary
        """
        return {
            "foreground": self.foreground,
            "background": self.background,
            "palette": self.palette
        }
    
    def set_foreground(self, foreground: str) -> None:
        """
            Normal Text Color
        """
        self.foreground = foreground

    def set_background(self, background: str) -> None:
        """
            Normal Background Color
        """
        self.background = background

    def set_pallete(self, palette: str) -> None:
        """
            List of 8 or 16 Colors, Separated By Colons (In CSS Hex Codes #rrggbb)
        """
        self.palette = palette

    def get_foreground(self) -> Optional[str]:
        """
            Normal Text Color
        """
        return self.foreground

    def get_background(self) -> Optional[str]:
        """
            Normal Background Color
        """
        return self.background

    def get_pallete(self) -> Optional[str]:
        """
            List of 8 or 16 Colors, Separated By Colons (In CSS Hex Codes #rrggbb)
        """
        return self.palette
    
    def __repr__(self) -> str:
        """
            The string that is displayed in interactive Python
        """
        return "Theme Of Format of Text Recording"
    
    def __str__(self) -> str:
        """
            Equivalent to Java's to_string()

            Prints string representation of object when printed
        """
        return json.dumps(self.get_theme())


class Format:
    version: Optional[int]  # At time of writing, latest version is 2 (required)
    width: Optional[int]  # Number of columns (required)
    height: Optional[int]  # Number of rows (required)
    timestamp: Optional[int] = None  # Start of Recording As Unix Timestamp
    duration: Optional[float] = None  # Duration of Recording When Known Up Front
    idle_time_limit: Optional[float] = None  # Maximum allowed period between frames when played back
    command: Optional[str] = None  # Command that was recorded
    title: Optional[str] = None  # Title of the recording
    env: Optional[dict] = None  # Environment at time of recording, should only contain SHELL and TERM
    theme: Optional[Theme] = None  # Color Theme of Recorded Terminal
    stdout: Optional[list] = None  # Array of Frames. Only exists in V1 Files

    def __init__(self, header: Optional[dict] = None) -> None:
        if header is not None:
            self.set_header(header=header)

    def set_header(self, header: dict) -> None:
        """
            Save Recording Header To Class
        """
        # Required Contents
        self.version = header["version"] if "version" in header else None
        self.width = header["width"] if "width" in header else None
        self.height = header["height"] if "height" in header else None

        # Optional Contents
        self.timestamp = header["timestamp"] if "timestamp" in header else None
        self.duration = header["duration"] if "duration" in header else None
        self.idle_time_limit = header["idle_time_limit"] if "idle_time_limit" in header else None
        self.command = header["command"] if "command" in header else None
        self.title = header["title"] if "title" in header else None
        self.theme = header["theme"] if "theme" in header else None

        # Can Contain Variable Contents Inside
        self.env = header["env"] if "env" in header else None

        # V1 Only Contents
        self.stdout = header["stdout"] if "stdout" in header else None

    def get_header(self) -> dict:
        """
            Recreate Recording Header as Dictionary
        """
        header: dict = {
            # Required
            "version": self.version,
            "width": self.width,
            "height": self.height
        }

        # Optional
        if self.timestamp is not None:
            header["timestamp"] = self.timestamp

        if self.duration is not None:
            header["duration"] = self.duration # type: ignore

        if self.idle_time_limit is not None:
            header["idle_time_limit"] = self.idle_time_limit # type: ignore

        if self.command is not None:
            header["command"] = self.command # type: ignore

        if self.title is not None:
            header["title"] = self.title # type: ignore

        if self.env is not None:
            header["env"] = self.env # type: ignore

        if self.theme is not None:
            header["theme"] = self.theme # type: ignore

        # V1 Only Contents
        if self.stdout is not None:
            header["stdout"] = self.stdout # type: ignore

        return header

    def get_version(self) -> Optional[int]:
        """
            At time of writing, latest version is 2

            See https://github.com/asciinema/asciinema/blob/develop/doc/asciicast-v2.md
            Also See https://github.com/asciinema/asciinema/blob/develop/doc/asciicast-v1.md
        """
        return self.version
    
    def get_width(self) -> Optional[int]:
        """
            Number of Columns
        """
        return self.width
    
    def get_height(self) -> Optional[int]:
        """
            Number of Rows
        """
        return self.height

    def get_timestamp(self) -> Optional[int]:
        """
            Start of Recording As Unix Timestamp
        """
        return self.timestamp
    
    def get_duration(self) -> Optional[float]:
        """
            Duration of Recording When Known Up Front
        """
        return self.duration
    
    def get_idle_time_limit(self) -> Optional[float]:
        """
            Maximum allowed period between frames when played back
        """
        return self.idle_time_limit
    
    def get_command(self) -> Optional[str]:
        """
            Command that was recorded
        """
        return self.command
    
    def get_title(self) -> Optional[str]:
        """
            Title of the recording
        """
        return self.title
    
    def get_environment(self) -> Optional[dict]:
        """
            Environment at time of recording, should only contain SHELL and TERM
        """
        return self.env
    
    def get_theme(self) -> Optional[Theme]:
        """
            Color theme of terminal
        """
        return self.theme
    
    def get_stdout(self) -> Optional[list]:
        """
            Array of Frames. Only exists in V1 Files
        """
        return self.stdout

    def set_version(self, version: int) -> None:
        """
            At time of writing, latest version is 2

            See https://github.com/asciinema/asciinema/blob/develop/doc/asciicast-v2.md
            Also See https://github.com/asciinema/asciinema/blob/develop/doc/asciicast-v1.md
        """
        self.version = version
    
    def set_width(self, width: int) -> None:
        """
            Number of Columns
        """
        self.width = width
    
    def set_height(self, height: int) -> None:
        """
            Number of Rows
        """
        self.height = height

    def set_timestamp(self, timestamp: Optional[int] = None) -> None:
        """
            Start of Recording As Unix Timestamp
        """
        self.timestamp = timestamp
    
    def set_duration(self, duration: Optional[float] = None) -> None:
        """
            Duration of Recording When Known Up Front
        """
        self.duration = duration
    
    def set_idle_time_limit(self, idle_time_limit: Optional[float] = None) -> None:
        """
            Maximum allowed period between frames when played back
        """
        self.idle_time_limit = idle_time_limit
    
    def set_command(self, command: Optional[str] = None) -> None:
        """
            Command that was recorded
        """
        self.command = command
    
    def set_title(self, title: Optional[str] = None) -> None:
        """
            Title of the recording
        """
        self.title = title
    
    def set_environment(self, env: Optional[dict] = None) -> None:
        """
            Environment at time of recording, should only contain SHELL and TERM
        """
        self.env = env

    def set_theme(self, theme: Optional[Theme]) -> None:
        """
            Color theme of terminal
        """
        self.theme = theme
    
    def set_stdout(self, stdout: Optional[list] = None) -> None:
        """
            Array of Frames. Only exists in V1 Files
        """
        self.stdout = stdout

    def __repr__(self) -> str:
        """
            The string that is displayed in interactive Python
        """
        return "Format Of Text Recording"
    
    def __str__(self) -> str:
        """
            Equivalent to Java's to_string()

            Prints string representation of object when printed
        """
        return json.dumps(self.get_header())


class Event:
    offset: Optional[float]  # Number of seconds since beginning of recording
    type: Optional[str]  # Current either o (for stdout) or i (for stdin) as of time of writing
    data: Any  # Can be anything JSON compatible, but for Asciinema i and o, must be UTF-8 string

    def __init__(self, event: Optional[Union[list, dict]] = None) -> None:
        if event is not None:
            self.set_event(event=event)

    def set_event(self, event: Union[list, dict]) -> None:
        if type(event) is list and len(event) == 3:
            self.offset = event[0]
            self.type = event[1]
            self.data = event[2]
        elif type(event) is dict:
            self.offset = event["offset"] if "offset" in event else None
            self.type = event["type"] if "type" in event else None
            self.data = event["data"] if "data" in event else None

    def get_event(self) -> list:
        return [
            self.offset,
            self.type,
            self.data
        ]
    
    def get_event_as_dict(self) -> dict:
        return {
            "offset": self.offset,
            "type": self.type,
            "data": self.data
        }

    def get_offset(self) -> Optional[float]:
        """
            Number of seconds since beginning of recording
        """
        return self.offset
    
    def get_type(self) -> Optional[str]:
        """
            Type of Event

            Either o (for stdout) or i (for stdin) when using Asciinema
        """
        return self.type
    
    def get_data(self) -> Any:
        """
            Can be anything JSON compatible, but for Asciinema i and o, must be UTF-8 string
        """
        return self.data
    
    def set_offset(self, offset: float) -> None:
        """
            Number of seconds since beginning of recording
        """
        self.offset = offset
    
    def set_type(self, type: str) -> None:
        """
            Type of Event

            Either o (for stdout) or i (for stdin) when using Asciinema
        """
        self.type = type
    
    def set_data(self, data: Any) -> None:
        """
            Can be anything JSON compatible, but for Asciinema i and o, must be UTF-8 string
        """
        self.data = data

    def __repr__(self) -> str:
        """
            The string that is displayed in interactive Python
        """
        return "Event Of Text Recording"
    
    def __str__(self) -> str:
        """
            Equivalent to Java's to_string()

            Prints string representation of object when printed
        """
        return json.dumps(self.get_event_as_dict())


class Recording:
    file_pointer: Union[TextIOWrapper, StringIO]
    file: Union[str, bytes, os.PathLike, TextIOWrapper, StringIO]
    header: Optional[Format] = None

    def __init__(self, file: Union[str, bytes, os.PathLike, TextIOWrapper, StringIO], header: Optional[Format] = None) -> None:
        self.file = file
        self.open_file()

        if header is None:
            self.read_header()

    def open_file(self) -> None:
        if type(self.file) is TextIOWrapper:
            self.file_pointer = self.file
        elif type(self.file) is StringIO:
            self.file_pointer = self.file
        elif type(self.file) is str or bytes or os.PathLike:
            self.file_pointer = open(file=self.file, mode="a+") # type: ignore

    def has_header(self) -> bool:
        self.file_pointer.seek(0, os.SEEK_SET)
        line: str = self.file_pointer.readline()

        try:
            header: Format = Format(header=json.loads(line))

            # Should this check for more than just the version?
            if header.version is None:
                return False
            elif header.width is None:
                return False
            elif header.height is None:
                return False
        except:
            return False

        return True

    def write_header(self) -> None:
        """
            Write Header To File From Format Class
        """
        if self.has_header():
            # TODO: Edit First Line In Place
            pass

        if self.header is None:
            return

        self.file_pointer.seek(0, os.SEEK_SET)
        self.file_pointer.write("%s\n" % json.dumps(self.header.get_header()))

    def read_header(self) -> Optional[Format]:
        """
            Read Header From File To Format Class
        """
        if not self.has_header():
            self.header = None

        self.file_pointer.seek(0, os.SEEK_SET)

        line: str = self.file_pointer.readline()
        try:
            self.header = Format(header=json.loads(line))
            return self.get_header()
        except ValueError:
            self.header = None
    
    def set_header(self, header: Format) -> None:
        """
            Set Header
        """
        self.header = header

    def get_header(self) -> Optional[Format]:
        """
            Retrieve Header
        """
        return self.header

    def read(self) -> Generator[Event, None, None]:
        """
            Read Rows
        """
        self.file_pointer.seek(0, os.SEEK_SET)
        
        # TODO: Determine If Should Check For Dict (Header), Vs List (Row) Instead
        skip_line: bool = True
        for line in self.file_pointer.readlines():
            # Skip Header Line
            if skip_line:
                skip_line: bool = False
                continue

            yield Event(event=json.loads(line))

    def write(self, event: Event, write_as_dict: bool = False) -> None:
        self.file_pointer.seek(0, os.SEEK_END)

        if write_as_dict:
            self.file_pointer.write("%s\n" % json.dumps(event.get_event_as_dict()))
        else:
            self.file_pointer.write("%s\n" % json.dumps(event.get_event()))

    def __repr__(self) -> str:
        """
            The string that is displayed in interactive Python
        """
        return "Text Recording"
    
    def __str__(self) -> str:
        """
            Equivalent to Java's to_string()

            Prints string representation of object when printed
        """
        return "Text Recording Of `%s`" % self.file

if __name__ == "__main__":
    beep: StringIO = StringIO()
    # beep.writelines([
    #     '{"version":2,"width":14,"height":1,"title":"Beep"}\n',
    #     '[1, "o", "Hello World!\\u0007\\n"]'  # This also works `{"offset":1,"type":2,"data":"Hello World!\\u0007\\n"}`
    #     ])

    rec: Recording = Recording(file=beep)
    # rec: Recording = Recording(file="/home/alexis/Desktop/write.cast")
    # rec: Recording = Recording(file="/home/alexis/Downloads/beep.cast")
    # rec: Recording = Recording(file="/home/alexis/Downloads/train.cast")
    # rec: Recording = Recording(file="/home/alexis/Desktop/download.cast")

    header: Format = Format()
    header.set_version(2)
    header.set_width(14)
    header.set_height(1)
    header.set_title("Hello")

    rec.set_header(header=header)
    rec.write_header()

    event: Event = Event()
    event.set_offset(1)
    event.set_type("o")
    event.set_data("Hello World!\u0007\n")
    rec.write(event=event)

    for event in rec.read():
        offset: Optional[float] = event.offset
        event_type: Optional[str] = event.type
        data: Any = event.data
        print(data, end="")