#!/usr/bin/env bash

# set -ex

# Ensure there are no errors.
pipenv run python -W ignore manage.py check
pipenv run python -W ignore manage.py makemigrations --dry-run --check


# Check flake
echo
echo "$(tput setaf 6)$(tput bold)Checking your code against the PEP8 style guide to ensure code quality..$(tput sgr 0)"

#pipenv run flake8 .
#code=$?

#if [ "$code" -eq "0" ]; then
#    echo
#    echo -e "$(tput setaf 2)$(tput bold)Code quality looks fine.. $(tput sgr 0)"
#    echo
#    echo
#    echo
#else
#    echo
#    echo "$(tput setaf 1)$(tput bold)Your changes violate PEP8 conventions! Please fix these and then push. $(tput sgr 0)"
#    echo
#fi;


## Check imports
echo
echo "$(tput setaf 6)$(tput bold)Checking if all imports are ordered properly..$(tput sgr 0)"

#pipenv run isort . --check-only --rr
#code=$?

#if [ "$code" -eq "0" ]; then
#    echo
#    echo -e "$(tput setaf 2)$(tput bold)Imports are ordered properly, continuing.. $(tput sgr 0)"
#    echo
#    echo
#    echo
#else
#    echo
#    echo "$(tput setaf 1)$(tput bold)Imports are not ordered correctly! Please fix these and then push. $(tput sgr 0)"
#    echo
#fi;


## Run tests
echo
echo "$(tput setaf 6)$(tput bold)Running all unit tests..$(tput sgr 0)"

pipenv run coverage run manage.py test -v 2 --noinput
code=$?

 if [ "$code" -eq "0" ]; then
     echo
     echo -e "$(tput setaf 2)$(tput bold)All unit tests passed. Continuing.. $(tput sgr 0)"
     echo
 else
     echo
     echo -e "$(tput setaf 1)$(tput bold)Your changes broke some unit tests, or code coverage dropped below $MIN_COVERAGE%! Please fix this and then push. $(tput sgr 0)"
 fi;

echo
echo "$(tput setaf 6)$(tput bold)Code coverage report..$(tput sgr 0)"
pipenv run coverage xml
