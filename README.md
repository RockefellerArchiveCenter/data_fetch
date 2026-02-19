# data_fetch

A service for fetching data used in discovery.

## Getting Started

If you have [git](https://git-scm.com/) and [Docker](https://store.docker.com/search?type=edition&offering=community) installed, using this repository is as simple as:

    
    git clone https://github.com/RockefellerArchiveCenter/data_fetch.git
    cd data_fetch
    docker build -t data_fetch .
    docker run data_fetch
    

## Service Flow

The service processes packages as follows:
- Marks the service instance as running
- Instantiates the necessary client for the desired data source
- If fetching updated objects, the service:
    - Fetches identifiers for all objects that been updated
    - Fetches data for updated objects
    - The data is sent to an SNS topic with an indication of whether the data should be indexed or removed from the index
- If fetching deleted objects, the service:
    - Fetches identifiers for all objects that been deleted
    - Sends the identifier to an SNS topic, indicating that it should be deleted from the index
- Sends a success message to an SNS topic
- Updates the last run time of the service
- Marks the service instance as not running

If errors are encountered during any of the above steps, the service:
- Sends a failure message to an SNS topic
- Marks service instance as not running


## Usage

This repository is intended to be deployed as an ECS Task in AWS infrastructure.

### License

This code is released under an [MIT License](LICENSE).

## Contributing

This is an open source project and we welcome contributions! If you want to fix a bug, or have an idea of how to enhance the application, the process looks like this:

1. File an issue in this repository. This will provide a location to discuss proposed implementations of fixes or enhancements, and can then be tied to a subsequent pull request.
2. If you have an idea of how to fix the bug (or make the improvements), fork the repository and work in your own branch. When you are done, push the branch back to this repository and set up a pull request. Automated unit tests are run on all pull requests. Any new code should have unit test coverage, documentation (if necessary), and should conform to the Python PEP8 style guidelines.
3. After some back and forth between you and core committers (or individuals who have privileges to commit to the base branch of this repository), your code will probably be merged, perhaps with some minor changes.

This repository contains a configuration file for git [pre-commit](https://pre-commit.com/) hooks which help ensure that code is linted before it is checked into version control. It is strongly recommended that you install these hooks locally by installing pre-commit and running `pre-commit install`.

## Tests
New code should have unit tests. Tests can be run using [tox](https://tox.readthedocs.io/).
