#!/usr/bin/env python3
"""
This script watches for changes in your Sphinx documentation and rebuilds the site when changes are detected.

Usage
-----
Run as follows::

    watchmedo shell-command --patterns="*.rst;*.html;*.css;*.js" --recursive --command='cd docs && make html' .

**OR** you can use this script to watch for changes in your Sphinx documentation::

    python reload_sphinx.py

Requirements
------------
The requirements are installed along with the `doc` extra requirements. For this script, you need:
    - watchdog

"""

import os
import sys
import time

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class ReloadHandler(FileSystemEventHandler):
    def __init__(self, command):
        self.command = command

    def on_modified(self, event):
        print(f"{event.src_path} has been modified, reloading...")
        os.system(self.command)


if __name__ == "__main__":
    path = "doc/*rst"  # Directory to watch (change to your Sphinx documentation directory)
    command = "cd doc && make html"  # Command to rebuild and serve your site
    event_handler = ReloadHandler(command)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)  # Keep the script running
    except KeyboardInterrupt:
        observer.stop()
        sys.exit(0)
    observer.join()
