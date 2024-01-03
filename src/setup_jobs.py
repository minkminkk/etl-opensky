from setuptools import setup

setup(
    name = "spark_jobs",
    version = "1.1",
    packages = ["jobs"],
    package_dir = {"jobs": "src/jobs"},
    install_requires = [
        "pyspark[sql]", 
        "pyspark[pandas_on_spark]", 
        "requests",
        "pyarrow"
    ]
)