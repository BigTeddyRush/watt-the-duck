from setuptools import setup, find_packages

setup(
    name="orchestrator",
    version="0.1.0",
    description="Energy Data Orchestrator",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    python_requires=">=3.9,<3.13",
    packages=find_packages(exclude=["infrastructure", "dbt_project"]),
    package_data={
        "orchestrator": [
            "dbt_projects/**/*",
        ],
    },
    install_requires=[
        "dagster==1.10.13",
        "dagster-cloud",
        "dagster-dbt",
        "dagster-embedded-elt",
        "dagster-powerbi",                 # fixed typo
        "dlt",
        "dlt[az]",
        "dbt-core>=1.8,<1.9",
        "dbt-duckdb>=1.8,<1.9",
        "duckdb>=0.10",
        "entsoe-py",
    ],
    extras_require={
        "dev": [
            "dagit",
            "dagster-webserver",
            "pytest",
        ]
    },
)
