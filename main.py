'''Event based folder rotator, use it for backing up save files that are prone to corruption. index 1 is the most
recent version. Each subsequent versions are chronologically old folders/archives.'''
import os
import sys
import time
import zlib
import py7zr
import logging
import argparse
from dataclasses import dataclass
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent, DirModifiedEvent

sys.path.append(os.path.abspath(__file__))
from idgen import idgen
from rotator import FileRotate


@dataclass
class Args:
    source: str
    dest: str
    count: int
    # no_compress: bool  # TODO
    delay: float


def eprint(*errors, **kwargs):
    '''prints errors to stderr'''
    print(*errors, file=sys.stderr, **kwargs)


def get_version() -> str:
    base_folder = os.path.dirname(os.path.abspath(__file__))
    version_file = os.path.join(base_folder, "VERSION.md")
    with open(version_file, "r") as f:
        return f.read()

    
def init_logs():
    logging.basicConfig(level=logging.INFO,
                        format='[%(levelname)s] %(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')


def parse_args() -> Args:
    version = get_version()
    parser = argparse.ArgumentParser(description=__doc__, prog="frotate")
    parser.add_argument("source", help="path to your save game folder that would require sync")
    parser.add_argument("dest", help="path to where you want the output saves")
    parser.add_argument("--count", "-c", type=int, help="number of maximum versions, defaults to 100", default=100)
    # parser.add_argument("--no-compress", "-n", help="don't compress", action="store_true")  # TODO
    parser.add_argument("--version", "-v", action="version", version=f"%(prog)s {version}")
    parser.add_argument("--delay",
                        "-d",
                        type=float,
                        help="number of seconds to wait after first observing a change, Defaults to 1.0",
                        default=1.0)
    arg_data = parser.parse_args()
    args_dict = vars(arg_data)
    return Args(**args_dict)

def validate(args):
    if (not os.path.isdir(args.dest)):
        eprint(f"Error: destination directory '{args.dest}' does not exist")
        sys.exit(1)
    
    if (not os.path.exists(args.source)):
        eprint(f"Error: source path '{args.source}' does not exist")
        sys.exit(2)


def update_checksums(source: str, checksums: dict) -> None:
    files = [
        os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(source)) for f in fn
    ]
    for file in files:
        with open(file, "rb") as f:
            checksums[file] = zlib.crc32(f.read())


def generate_staging_archive(args: Args, id_generator: idgen) -> str:
    start_time = time.time()
    id = id_generator.generate()
    name = f"staging-{id}.7z"
    path = os.path.join(args.dest, name)
    with py7zr.SevenZipFile(path, 'w') as archive:
        archive.writeall(args.source)
    elapse = round(time.time() - start_time, 3)
    logging.info(f"Staging archive generated: took {elapse}s. Beginning rotation, DO NOT QUIT!")
    return name


def handler_factory(args: Args):
    checksums = {}
    lock = False  # TODO: make a more sophisticated lock than this
    rotator = FileRotate(args.dest, args.count, "7z", eprint)
    id_generator = idgen(10)
    update_checksums(args.source, checksums=checksums)
    def handler(event: FileModifiedEvent):
        nonlocal lock
        nonlocal checksums
        nonlocal rotator
        nonlocal id_generator
        if lock or type(event) == DirModifiedEvent:
            return
        new_checksums = {}
        update_checksums(args.source, checksums=new_checksums)
        if new_checksums == checksums:
            return

        lock = True
        time.sleep(args.delay)
        checksums = new_checksums
        logging.info(f"Modification detected on '{event.src_path}', backing up directory...")
        name = generate_staging_archive(args, id_generator)

        status = rotator.add_file(name)
        if status:
            logging.info(f"Successfully rotated. Continuing to watch.")
        else:
            logging.warn(f"noop, possible hash collision (duplicate). Continuing to watch.")

        checksums = new_checksums
        lock = False
    return handler


def main():
    args = parse_args()
    validate(args)
    init_logs()
    observer = Observer()
    event_handler = FileSystemEventHandler()
    event_handler.on_modified = handler_factory(args)
    observer.schedule(event_handler, args.source, recursive=True)
    observer.start()
    logging.info(f"Watchdog has begun observing.")
    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join(1)
        logging.info("exiting")


if __name__ == '__main__':
    main()