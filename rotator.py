import os
import time
import zlib
from typing import Optional


class FileRotate:
    _max_count: int
    _count: int = 0
    _ext: str
    _manifest_index: dict[str: list[str]]  # { file_name: [ creation_date, hash ] }
    _last_seen_hash: int = 0

    def __init__(self, 
                 path: str, 
                 count: int,
                 ext: str,
                 eprint: Optional[callable] = None) -> None:
        self._path = path
        self._eprint = eprint
        self._max_count = count
        self._ext = ext
        self._manifest_index = {}
        manifest_path = os.path.join(path, "manifest.csv")
        
        if (not os.path.exists(manifest_path)):
            with open(manifest_path, "w+") as f: pass
            self._count = 0
            return
        
        final_version_number = -1
        with open(manifest_path, "r") as manifest:
            for line in manifest.readlines():
                tokenized = [ word.strip() for word in line.split(",") ]
                version_name = tokenized[0]
                self._manifest_index[version_name] = tokenized[1:]
                self._count += 1
                version_number = int(version_name.split(".")[0])
                if final_version_number == -1 or version_number < final_version_number:
                    final_version_number = version_number
                    self._last_seen_hash = int(tokenized[2])


    # return true on success, false on fail such as similar hash
    def add_file(self, filename: str) -> bool:
        '''
        add a file to backup directory using this method.
        
        Arguments:
            filename (str): filename, without directory path.

        Returns:
            True if successfully writes, False when something occurs or hash collision
        '''
        src_path = os.path.join(self._path, filename)
        with open(src_path, "rb") as archive:
            target_hash = zlib.crc32(archive.read())

        if target_hash == self._last_seen_hash:
            return False  # duplicate, noop
        
        self._shift_index()
        self._last_seen_hash = target_hash
        self._manifest_index[f"1.{self._ext}"] = [
            self._current_time,
            str(self._last_seen_hash)   # cast to string for consistency
        ]
        os.rename(src_path, os.path.join(self._path, f"1.{self._ext}"))
        if self._count < self._max_count:
            self._count += 1
            self._write_to_manifest()
            return True

        del self._manifest_index[f"{self._count+1}.{self._ext}"]
        os.remove(os.path.join(self._path, f"{self._count+1}.{self._ext}"))
        self._write_to_manifest()
        return True

        
    def _shift_index(self) -> None:
        ## shift current save files by 1 index and update _manifest_index
        for index in range(self._count, 0, -1):
            current_key = f"{index}.{self._ext}"
            target_key = f"{index+1}.{self._ext}"
            current_path = os.path.join(self._path, current_key)
            target_path = os.path.join(self._path, target_key)
            self._manifest_index[target_key] = self._manifest_index[current_key]
            os.rename(current_path, target_path)
        if self._count > 0:
            del self._manifest_index[f"1.{self._ext}"]


    @property
    def _current_time(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S")
    
    def _write_to_manifest(self) -> None:
        manifest_path = os.path.join(self._path, "manifest.csv")
        with open(manifest_path, "w") as manifest:

            for i in range(1, self._count+1):
                key = f"{i}.{self._ext}"
                timestamp = self._manifest_index[key][0]
                hash = self._manifest_index[key][1]
                text_line = f"{key},{timestamp},{hash}\n"
                manifest.write(text_line)

if __name__ == "__main__":
    ## local unit tests
    rotator = FileRotate("/tmp/rotator-test/dest", 10, "7z", None)
    rotator.add_file("staging.7z")