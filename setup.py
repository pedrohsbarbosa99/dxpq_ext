from setuptools import Extension, setup


extension = Extension(
    "dxpq_ext",
    sources=[
        "src/dxpq_ext.c",
        "src/connection.c",
        "src/cursor.c",
    ],
    libraries=["pq"],
    library_dirs=["/usr/lib"],
    include_dirs=["/usr/include/", "/usr/include/postgresql/"],
)

setup(
    name="dxpq_ext",
    version="0.0.1",
    ext_modules=[extension],
)
