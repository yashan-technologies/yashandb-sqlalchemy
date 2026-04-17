from setuptools import setup
import os
import platform

from setuptools import setup, find_packages


def get_project_path():
    return os.path.dirname(os.path.abspath(__file__))


def get_version():
    if platform.system() == 'Windows':
        null_path = 'nul'
    else:
        null_path = '/dev/null'

    os.chdir(get_project_path())
    describe = "".join(os.popen("git describe --tags 2>{}".format(null_path)).readlines()).strip()
    return describe if describe else "unknow"


setup(
    name="yashandb-sqlalchemy",
    version=get_version(),
    description="YashanDB Dialect for SQLAlchemy",
    author="CoD",
    author_email="cod@sics.ac.cn",
    url="https://www.yashandb.com/",
    license="MulanPSL-2.0",
    packages=["yashandb_sqlalchemy"],
    include_package_data=True,
    entry_points={
        "sqlalchemy.dialects": [
            "yashandb = yashandb_sqlalchemy.yaspy:YasDialect_yaspy",
            "yashandb.yaspy = yashandb_sqlalchemy.yaspy:YasDialect_yaspy",
            "yashandb.yasdb = yashandb_sqlalchemy.yasdb:YasDialect_yasdb",
        ]
    },
    
)
