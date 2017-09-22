#!/usr/bin/python
#
# Works with either Python v2.7+ or v3.3+
#
"""Summary
-------

This module provides an extension to the standard tarfile.TarFile class
which provides the ability to add files to a TarFile that are compressed
on-the-fly.  It will only work on an uncompressed output tarfile, since after
the data is written it will overwrite the header for the file with the
correct size data.

Limitations
-----------

- The object to which the tarfile is being written must support "seek()", so
  this cannot work over a socket, nor presumably with a compressed tarfile.
  *Note: re-compressing contents is not very useful.*

- The "close_gz_file" method will be called when calling "close" on the
  file stream.
  *Note: close_gz_file() and close_file() are interchangeable.*

Example Usage
-------------

.. code:: python

    #!/usr/bin/env python3
    import os, sys, shutil

    import targzstream

    # USAGE:  ./foo.py TARFILE INPUT [ INPUT2 ... ]
    #  Eg: ./foo.py myoutput.tar *.cpp *.h

    with targzstream.TarFile(sys.argv[1], mode='w') as tarball:
        for fname in sys.argv[2:]:
            st = os.stat(fname)
            with tarball.add_gz_file(name=fname + '.gz', mtime=st.st_mtime,
                                     uid=st.st_uid, gid=st.st_gid, mode=st.st_mode) as fout:

                # Copy the data.
                with open(fname, 'rb') as fin:
                    shutil.copyfileobj(fin, fout)
    # The end.

TODO
----

- Wrap *add_gz_file* and *close_gz_file* as a context manager, allowing simply:

  *Done.*

- Allow streaming uncompressed files, too.

  *Done.*

- Have *add_gz_file* handle the result of an *os.stat*.  Eg:

  .. code:: python

    with tarball.gz_file(name=fname + '.gz', stat=os.stat(fname)) as obj:
        with open(fname, 'rb') as fin:
            shutil.copyfileobj(fin, obj)
"""
import gzip
import logging
import sys
import tarfile
import time

__version__ = "1.1"
__author__ = "NVRAM (nvram@users.sourceforge.net)"
__date__ = "Fri Sep 21 22:14:23 MDT 2017"
__credits__ = "NVRAM"
__descr__ = ('An extension to tarfile to allow adding files to a tarfile, without the ' +
             'need to write to disk first.  It also allows data to be compressed as it ' +
             'is added to the tarfile, for large files or data that might be generated ' +
             'on the fly.  Note that the output file object must support "seek()", ' +
             'hence the output must be an uncompressed tar file.  Currently, only ' +
             'GZip is supported for compression.')

_OLD_SCHOOL = sys.version_info.major == 2


class GzipStream(object):
    def __init__(self, name, fdes, onclose, level=9, mtime=None, encoding='utf-8'):
        self.fdes = fdes
        self.onclose = onclose
        self.encoding = encoding
        self.start = fdes.tell()
        if level >= 0:
            self.stream = gzip.GzipFile(filename=name, compresslevel=level, mode='wb',
                                        fileobj=fdes, mtime=mtime)
            self.compressed = True
        else:
            self.stream = fdes
            self.compressed = False

    def write(self, data):
        if not _OLD_SCHOOL and isinstance(data, str):
            data = bytes(data.encode(self.encoding))
        self.stream.write(data)

    def flush(self):
        pass

    def close(self, only=False):
        if not only:
            return self.onclose()
        if self.compressed:
            self.stream.close()
        self.end = self.fdes.tell()
        self.size = self.end - self.start
        return self.size

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if type is None:
            self.close()


class TarFile(tarfile.TarFile):
    def __init__(self, name, mode='w', *args, **kwds):
        if mode[0] not in ('w', 'x', 'a'):
            raise ValueError("Mode '%s' is not allowed." % mode)
        super(TarFile, self).__init__(name, mode, *args, **kwds)
        self.__reset()

    @classmethod
    def open(cls, name, mode='r', fileobj=None, bufsize=tarfile.RECORDSIZE, **kwargs):
        if mode.startswith('r'):
            return tarfile.TarFile.open(name=name, mode=mode, fileobj=fileobj,
                                        bufsize=bufsize, **kwargs)
        return cls(name, mode, **kwargs)

    def __reset(self):
        self.__stream = None
        self.__currinfo = None
        self.__location = None
        self.__compressed = None

    def __writeheader(self):
        buff = self.__currinfo.tobuf(self.format, self.encoding)
        self.fileobj.write(buff)

    def add_file(self, name, mtime, **stats):
        logging.debug("Adding file: %s", name)
        return self._do_add(name, False, mtime, stats)

    def close_gz_file(self):
        return self.close_file()

    def add_gz_file(self, name, mtime, **stats):
        logging.debug("Adding GZ file: %s", name)
        return self._do_add(name, True, mtime, stats)

    def _do_add(self, name, compress, mtime, stats):
        if self.__currinfo:
            self.close_gz_file()
        tinfo = tarfile.TarInfo(name=name)
        tinfo.mtime = mtime or time.time()
        for key, value in stats.items():
            if hasattr(tinfo, key):
                setattr(tinfo, key, value)
        self.fileobj.flush()
        self.__location = self.fileobj.tell()
        self.__currinfo = tinfo
        self.__writeheader()
        self.__compressed = compress
        self.__stream = GzipStream(name, self.fileobj, onclose=self.close_gz_file,
                                   level=9 if compress else -1, mtime=mtime)
        return self.__stream

    def close_file(self):
        self.__stream.flush()
        # Raw close on the GZ stream.
        self.__currinfo.size = self.__stream.close(only=True)
        logging.debug("Closing file: %s (%d)", self.__currinfo.name, self.__currinfo.size)

        end = self.fileobj.tell()
        _, extra = divmod(end, tarfile.BLOCKSIZE)
        padding = tarfile.BLOCKSIZE - extra
        if padding > 0:
            self.fileobj.write(b'\0' * padding)
            self.fileobj.flush()
            end = self.fileobj.tell()

        self.fileobj.seek(self.__location, 0)
        self.__writeheader()
        self.fileobj.seek(0, 2)
        self.__reset()

    def close(self):
        if self.__currinfo:
            self.close_gz_file()
        super(TarFile, self).close()


open = TarFile.open
