# README.MD - epipipeline v3.0.2

This package contains various functions and tools to fetch, preprocess, and standardise epidemiological data.

## Usage

To use this package in your environment to modify epidemiological data - it's recommended you use [poetry](̌https://python-poetry.org/). You can install poetry using the steps listed in their official documentation. It's also recommended you use a conda environment as a wrapper, but you are free to use other virtual environments as per your preference. The instructions below assume usage of ```poetry``` and ```conda```.

```epipipeline``` requires ```python>=3.9```.

You can install and use it so:

```bash
cd my-project
conda create -n my-project-dev python=3.9 -y
conda activate my-project-dev
poetry init # follow the steps suggested
# Install from git using https
poetry add git+https://github.com/dsih-artpark/epipipeline.git
# OR install using SSH, which might require additional authentication if you have SSH keys set up.
poetry add git+ssh://git@github.com:dsih-artpark/epipipeline.git
poetry lock # lock epipipeline as a project dependency
```

It is not recommended that you use pip to install this package, but if you prefer to do that, use:
```bash
pip install git+https://github.com/dsih-artpark/epipipeline.git
```

After this, you can use ```epipipeline``` as a regular package in your environment. 

```python
from epipipeline.preprocess.dengue.karnataka import fetch_ka_linelist_v2, preprocess_ka_linelist_v2
from epipipeline.standardise.dengue.karnataka import standardise_ka_linelist_v3

from epipipeline.standardise import standardise_age, standardise_gender
```

## Contributing

To contribute, if you have access to create branches, you can clone and create a pull request with your changes. Else, you can do the same from a [fork](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo).

```bash
git clone https://github.com/dsih-artpark/epipipeline.git

git checkout -b sk-gis-feat
# sk stands for your initials, e.g. mine stand for Sneha Kanmani. 
# Be sure to mention what the feature or patch is about.

# Make your changes to the code and save them

# Add and commit your changes.
git add epipipeline.standardise.gis.py
git commit -m "feat: added functionality to take a shapefile and return lat-lon bounds"
git push -u origin sk-gis-feat
```

For commit messages, we recommend using the [Angular Convention on Commits](https://www.conventionalcommits.org/en/v1.0.0-beta.4/).

After that, create a pull request on github.com or using the ```github cli``` (official documentation [here](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request?tool=cli)). 