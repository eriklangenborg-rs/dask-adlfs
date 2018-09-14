from __future__ import print_function, division, absolute_import

import logging
from azure.datalake.store import lib, AzureDLFileSystem

from dask.bytes import core
from dask.bytes.utils import infer_storage_options
from dask.base import tokenize

logger = logging.getLogger(__name__)


class DaskAdlFileSystem(AzureDLFileSystem):
    """API spec for the methods a filesystem

    A filesystem must provide these methods, if it is to be registered as
    a backend for dask.

    Implementation for Azure Data Lake store
    """
    sep = '/'

    def __init__(self, tenant_id=None, client_id=None, client_secret=None,
                 **kwargs):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.store_name = kwargs['host']
        self.kwargs = kwargs
        self.kwargs['store_name'] = kwargs['host']
        logger.debug("Init with kwargs: %s", self.kwargs)
        self.do_connect()

    def do_connect(self):
        token = lib.auth(tenant_id=self.tenant_id,
                         client_id=self.client_id,
                         client_secret=self.client_secret)
        self.kwargs['token'] = token
        AzureDLFileSystem.__init__(self, **self.kwargs)

    def _trim_filename(self, fn):
        so = infer_storage_options(fn)
        return so['path']

    def glob(self, path):
        """For a template path, return matching files"""
        adl_path = self._trim_filename(path)
        return ['adl://%s.azuredatalakestore.net/%s' % (self.store_name, s)
                for s in AzureDLFileSystem.glob(self, adl_path)]

    def mkdirs(self, path):
        pass  # no need to pre-make paths on ADL

    def open(self, path, mode='rb'):
        adl_path = self._trim_filename(path)
        f = AzureDLFileSystem.open(self, adl_path, mode=mode)
        return f

    def ukey(self, path):
        adl_path = self._trim_filename(path)
        return tokenize(self.info(adl_path)['modificationTime'])

    def size(self, path):
        adl_path = self._trim_filename(path)
        return self.info(adl_path)['length']

    def _get_pyarrow_filesystem(self):
        from pyarrow.util import implements
        from pyarrow.filesystem import FileSystem, DaskFileSystem
        from azure.datalake.store.core import AzureDLPath
        import posixpath
        import os
        class AzureDLFSWrapper(DaskFileSystem):

            @implements(FileSystem.isdir)
            def isdir(self, path):
                apath = AzureDLPath(path).trim().as_posix()
                try:
                    contents = self.fs.ls(apath)
                    if len(contents) == 1 and contents[0] == apath:
                        return False
                    else:
                        return True
                except OSError:
                    return False

            @implements(FileSystem.isfile)
            def isfile(self, path):
                apath = AzureDLPath(path).trim().as_posix()
                try:
                    contents = self.fs.ls(apath)
                    return len(contents) == 1 and contents[0] == apath
                except OSError:
                    return False

            def walk(self, path, invalidate_cache=True):
                """
                Directory tree generator, like os.walk

                Generator version of what is in adlfs, which yields a flattened list of
                files
                """
                directories = set()
                files = set()

                for apath in list(self.fs._ls(path, invalidate_cache=invalidate_cache)):
                    if apath['type'] == 'DIRECTORY':
                        directories.add(apath['name'])
                    elif apath['type'] == 'FILE':
                        files.add(apath['name'])
                    else:
                        pass

                files = sorted([posixpath.split(f)[1] for f in files
                                if f not in directories])
                directories = sorted([posixpath.split(x)[1]
                                    for x in directories])

                yield path, directories, files

                for directory in directories:
                    for tup in self.walk(directory, invalidate_cache=invalidate_cache):
                        yield tup

        return AzureDLFSWrapper(self)

    def __getstate__(self):
        dic = self.__dict__.copy()
        del dic['token']
        del dic['azure']
        logger.debug("Serialize with state: %s", dic)
        return dic

    def __setstate__(self, state):
        logger.debug("De-serialize with state: %s", state)
        self.__dict__.update(state)
        self.do_connect()

core._filesystems['adl'] = DaskAdlFileSystem
