from setuptools import Extension, setup
import os

extension = Extension(
    "dxpq_ext",
    sources=[
        "src/dxpq_ext.c",
        "src/connection.c",
        "src/cursor.c",
    ],
    libraries=["pq"],
    library_dirs=["/usr/lib"],
    include_dirs=[
        "/usr/include/",
        "/usr/include/postgresql/",
        os.path.join(os.path.dirname(__file__), "src"),
    ],
)

setup(
    name="dxpq_ext",
    version="0.0.2",
    ext_modules=[extension],
    description="Extension for PostgreSQL interaction",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Pedro Barbosa",
    author_email="pedrohsbarbosa99@gmail.com",
    url="https://github.com/pedrohsbarbosa99/dxpq_ext",
    packages=["src"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    package_data={"": ["*.h"]},
    include_package_data=True,
)
