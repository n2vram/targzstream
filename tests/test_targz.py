# Tests for the targz module.

import gzip
import logging
import os
import pytest
import random
import tarfile

# Code under test:
import targzstream

logging.basicConfig(level=logging.DEBUG)


def test_disallow_compression(tmpdir):
    """The resulting tarfile cannot be streamed nor compressed, unless it's reading."""
    base = tmpdir.mkdir("1")

    for mode in ('w:bz2', 'w:gz', 'w|bz2', 'w|gz', 'x:bz2', 'x:gz'):

        path = base.join("outfile.tar." + mode[2:])

        with pytest.raises(ValueError) as raised:
            targzstream.open(path, mode=mode)

        with pytest.raises(ValueError) as raised:
            targzstream.TarFile.open(path, mode=mode)

    logging.info("OPEN_METH = %s", targzstream.TarFile.OPEN_METH)

    # Allow compressed reading, but we expect an exception
    localpath = base.join("outfile-nosuch.tar")
    path = str(localpath)
    for mode in ('r', 'r:*', 'r:', 'r|', 'r:bz2', 'r:gz', 'r|*', 'r|bz2', 'r|gz'):

        with pytest.raises((OSError, IOError)) as raised:
            targzstream.open(path, mode=mode)
        assert raised.value.errno == 2

        with pytest.raises((OSError, IOError)) as raised:
            targzstream.TarFile.open(path, mode=mode)
        assert raised.value.errno == 2


def test_readback(tmpdir):
    base = tmpdir.mkdir("2")
    tfile = base.join("test2.tar")
    rand = random.Random()

    tarball = None

    files = {}
    actions = [('medium', 9999, 1492000015),
               ('emptyfile', 0, 1492000030),
               ('smaller', 380, 1492000045),
               ('biggy', 49999, 1492000060),
               ('little', 1055, 1492010015),
               ('emptiness', 0, 1492010030),
               ('large', 10240, 1492010060),
               ('one-byter', 1, 1492010045)]

    # Open first, re-open in append mode after 3 files:
    actions.insert(0, "w")
    actions.insert(4, "a")
    actions.append(None)
    logging.info("---- ACTIONS ----")
    for num, action in enumerate(actions):
        logging.info("Step %02d: %s", num, action)

    for action in actions:
        if isinstance(action, str):
            if tarball:
                tarball.close()
            tarball = targzstream.open(str(tfile), mode=action)
            os.system("ls -l '%s'" % tfile)
            logging.info("Opened(%s, mode='%s')  => %s",
                         tfile, action, tarball.fileobj.tell())
            continue

        if action is None:
            name = 'targzstream.py'
            stat = os.stat(name)
            mtime = int(stat.st_mtime)
            uid = stat.st_uid
            gid = stat.st_gid
            size = stat.st_size
            data = open(name).read()
            assert data[-1] == '\n', "%s does not end in a newline!" % name
            files[name] = [size, mtime, uid, gid, data.encode()]
            try:
                target = tarball.add_gz_file(name, mtime=mtime, uid=uid, gid=gid)
                for line in data[:-1].split('\n'):
                    target.write(line + '\n')
            finally:
                tarball.close_gz_file()
            continue

        name, size, mtime = action
        uid = mtime % 127
        gid = mtime % 213
        data = []
        files[name] = [size, mtime, uid, gid]

        try:

            # Write in pieces, maybe.
            logging.info("@%05x Target(%s) ...", tarball.fileobj.tell(), name)
            target = tarball.add_gz_file(name, mtime=mtime, uid=uid, gid=gid)
            logging.info("     ==> [%s]: %s", target.__class__.__name__, target)
            num = 0
            while num < size:
                psize = rand.randint(2, 13) ** 3 + rand.randint(5, 48)
                psize = min(size - num, psize * psize, psize * psize)
                chunk = os.urandom(psize)
                target.write(chunk)
                logging.info("        +%5d bytes (%d)", len(chunk), psize)
                data.append(chunk)
                num += psize
                if num > 300:
                    target.flush()

        except Exception as exc:
            logging.exception("Failed: %s" % exc)
            raise

        finally:
            if rand.randint(1, 100) < 85:
                logging.info("NOT Closing gz stream(%s)...", name)
            else:
                logging.info("Closing gz stream(%s)...", name)
                tarball.close_gz_file()

        files[name].append(b''.join(data))
        logging.info("++" * 80)
        os.system("ls -l '%s' >&2" % tfile)
        os.system("tar -Rtvvvf '%s' >&2" % tfile)
        logging.info("--" * 80)

    tarball.close()

    def verify(tarball):
        results = []
        for info in tarball:
            name = info.name
            mtime = info.mtime
            uid = info.uid
            gid = info.gid
            logging.info("Found '%s' @%04x mt=%s id=%s/%s",
                         name, tarball.fileobj.tell(), mtime, uid, gid)

            # Read the compressed data.
            fobj = tarball.extractfile(info)
            data = gzip.GzipFile(fileobj=fobj).read()

            # Strip the '.gz':
            expect = files[name]
            logging.info("Verifying %d bytes of '%s'", len(data), name)
            assert expect[:-1] == [len(data), mtime, uid, gid]
            assert expect[-1] == data
            results.append(name)

        print("Expect: %s" % sorted(files))
        print("Result: %s" % sorted(results))
        assert set(files) == set(results)

    # Read it all back...
    verify(tarfile.open(str(tfile), 'r'))
    verify(tarfile.TarFile.open(str(tfile), 'r'))
