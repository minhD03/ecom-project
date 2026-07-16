from setuptools import find_packages, setup

setup(
    name="ecom_dagster",
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "ecom_dagster": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-snowflake<1.10",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)