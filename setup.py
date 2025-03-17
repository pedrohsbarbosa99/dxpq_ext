from setuptools import Extension, setup

pg_config_include_dir = "/usr/include"
pg_config_lib_dir = "/usr/lib"


extension = Extension(
    "dxpq_ext",
    sources=["src/dxpq_ext.c", "src/connection.c", "src/cursor.c"],
    libraries=["pq"],
    library_dirs=[pg_config_lib_dir],
    include_dirs=[pg_config_include_dir],
)

setup(
    name="dxpq_ext",
    version="0.0.1",
    ext_modules=[extension],
)
