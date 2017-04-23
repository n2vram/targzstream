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

- The "close_gz_file" method *must* be called when the data is finished, and
  calling "close" on the *GzipStream* object is not sufficient.

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
            obj = tarball.add_gz_file(name=fname + '.gz', mtime=st.st_mtime,
                                      uid=st.st_uid, gid=st.st_gid, mode=st.st_mode)

            # Copy the data.
            with open(fname, 'rb') as fin:
                shutil.copyfileobj(fin, obj)

            # REMEMBER: close_gz_file() is required
            tarball.close_gz_file()
    # The end.

TODO
----

- Wrap *add_gz_file* and *close_gz_file* as a context manager, allowing simply:

  .. code:: python

    with tarball.gzstream(name=fname + '.gz', mtime=mtime, ...) as obj:
        with open(fname, 'rb') as fin:
            shutil.copyfileobj(fin, obj)

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

__version__ = "1.0"
__author__ = "NVRAM (nvram@users.sourceforge.net)"
__date__ = "Sun Apr 16 00:43:13 MDT 2017"
__credits__ = "NVRAM"
__descr__ = ('An extension to tarfile to allow adding compressed-on-the-fly files to ' +
             'a tarfile, allowing files too large to fit into memory or data that is ' +
             'generated on the fly.  Note that the output file object must support ' +
             '"seek()", hence must be a regular uncompressed tar file.')

_OLD_SCHOOL = sys.version_info.major == 2


class GzipStream(object):
    def __init__(self, name, fdes, level=9, mtime=None):
        self.fdes = fdes
        self.start = fdes.tell()
        self.gzip = gzip.GzipFile(filename=name, compresslevel=level, mode='wb',
                                  fileobj=fdes, mtime=mtime)

    def write(self, data):
        if isinstance(data, str) and not _OLD_SCHOOL:
            data = data.encode()
        self.gzip.write(data)

    def flush(self):
        pass

    def close(self):
        self.gzip.close()
        self.end = self.fdes.tell()
        self.size = self.end - self.start
        return self.size


class TarFile(tarfile.TarFile):
    def __init__(self, name, mode='w', *args, **kwds):
        if mode[0] not in ('w', 'x', 'a'):
            raise ValueError("Mode '%s' is not allowed." % mode)
        print("Calling tarfile.TarFile.__init__(name='%s', mode='%s', *%s, **%s) ..." %
              (name, mode, args, kwds))
        super(TarFile, self).__init__(name, mode, *args, **kwds)
        self.__reset()

    @classmethod
    def open(cls, name, mode='r', fileobj=None, bufsize=tarfile.RECORDSIZE, **kwargs):
        if mode.startswith('r'):
            parent = tarfile.TarFile
            print("%s(cls=%s, name='%s', mode='%s', fileobj=%s, bufsize=%s, %s)" %
                  (parent.open, parent, name, mode, fileobj, bufsize, kwargs))
            return tarfile.TarFile.open(name=name, mode=mode, fileobj=fileobj,
                                        bufsize=bufsize, **kwargs)
        return cls(name, mode, **kwargs)

    def __reset(self):
        self.__stream = None
        self.__currinfo = None
        self.__location = None

    def __writeheader(self):
        buff = self.__currinfo.tobuf(self.format, self.encoding)
        self.fileobj.write(buff)

    def add_gz_file(self, name, mtime, **stats):
        logging.debug("Adding GZ file: %s", name)
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
        self.__stream = GzipStream(name, self.fileobj, mtime=mtime)
        logging.debug("Wrote header(%s) between %04x - %04x", name, self.__location, self.fileobj.tell())
        return self.__stream

    def close_gz_file(self):
        print("Entering TarFile.close_gz_file() ....")
        self.__currinfo.size = self.__stream.close()
        logging.debug("Closing GZ file: %s (%d)", self.__currinfo.name, self.__currinfo.size)
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
