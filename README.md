# Collaborator Monitoring

This repo collects some work I have done to collect and analyse a dataset of coauthors of the research leaders at the Sydney Precision Data Science centre.

## Contents

The `src` directory contains the scripts and functions that I have utilised to query the scopus API and do some initial analysis on the data. 

> **Note**: The functions that I have created are documented and type annotated for clarity  and can be used separtely.

The `data` directory contains a JSON file which summarises all the relavant details of the resaerch leaders. Additionally, it contains a pregenerated dataset resulting from a previous run, this is likely to be out of date.

## Dependencies

You can install all the project dependencies in a virtual environment with [Pipenv](https://pipenv.pypa.io/en/latest/index.html#install-pipenv-today). After installing Pipenv, run the following:

```sh
pipenv sync
```


## Usage

Firstly, you must create an [Elsevier API key](https://dev.elsevier.com/) and store it in the project root directory in a file named `.scopus_api`. This key will be used to query the Scopus API.

Next, modify `data/research_leaders.json` with the details of the researchers that you would like to create a coauthorship dataset about.

Finally, run the following in a terminal window. This will overwrite the dataset in `data/collaborator_data.csv` with updated data based on your modified JSON file.

```sh
python src/collaborators.py
```

> **Note**: To ensure that you are running the code in the correct virtual environemnt, you can run `pipenv shell` in the project root directory. This will spawn a subshell correctly configured with the required dependencies.