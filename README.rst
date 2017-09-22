targzstream
===========

Version: 1.1

Summary
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

- The constructor does not support reading, use the `open()` class method or
  use the base class `tarfile.TarFile` constructor.


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

- Have *add_gz_file* handle the result of an *os.stat*.  Eg:

  .. code:: python

    with tarball.gz_file(name=fname + '.gz', stat=os.stat(fname)) as obj:
        with open(fname, 'rb') as fin:
            shutil.copyfileobj(fin, obj)

- Wrap *add_gz_file* and *close_gz_file* as a context manager.

  *Done.*

- Allow streaming uncompressed files, too.

  *Done.*

