radio-star
==============================

[![Build Status](https://travis-ci.com/woodytwoshoes/Radio-Star.svg?branch=main)](https://travis-ci.com/woodytwoshoes/Radio-Star)



An app used to fetch and present helpful images in radiology

Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── notebooks          <- Jupyter notebooks. Experiments, records, images, presentations.
    │                         
    │                         
    ├── setup.py           <- makes project pip installable (pip install -e .) so radio-star-demo can be imported
    ├── radio-star-demo                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes radio-star-demo a Python module
    │   │
    │   ├── data           <- data as it flows through the luigi pipeline
    │   │   ├── external       <- Data from third party sources.
    │   │   ├── interim        <- Intermediate data that has been transformed.
    │   │   ├── processed      <- The final, canonical data sets for modeling.
    │   │   └── raw            <- The original, immutable data dump.
    │   │
    │   │
    │   ├── models                   
    │   │   ├── helper functions <- Scripts to assist in doing vector calculations on dataframes, and manipualting images
    │   │   └── Tasks <-- Tasks which can be run in Luigi pipeline to achieve analysis and display results.
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py <- script to generate PCA visualization of results


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
