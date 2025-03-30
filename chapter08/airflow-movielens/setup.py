#!/usr/bin/env python
import setuptools

requirements = ["apache-airflow", "requests"]

setuptools.setup(
    name="airflow_movielens",
    version="0.1.0",
    description="Custom Airflow operators and hooks for the Movielens API",
    author="jeontable",
    author_email="jeontable@gmail.com",
    install_requires=requirements,
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    url="https://github.com/jeontable/data-pipelines-with-apache-airflow",
    license="MIT license"
)