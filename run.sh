#!/bin/bash

if [[ "${BASH_SOURCE-}" != "$0" ]]; then
  echo "this script should not be sourced, and must executed in a new shell"
  return
fi

set -e

SCRIPT_NAME=$(echo "$0" | awk -F/ '{print $NF}')
SCRIPT_PATH=$(cd "${0:0:-$(($(echo /"${SCRIPT_NAME}" | wc -c) - 1))}" && pwd)

cd "$SCRIPT_PATH"

##

[[ -f ".env" ]] && source .env

case $1 in

'')
  echo "test"
  echo "build"
  echo "clean"
  echo "publish_pypi_test"
  echo "publish_pypi"
  echo "publish_private"
  echo "install"
  echo "update"
  ;;

test)
  pytest
  ;;

build)
  poetry build
  ;;

clean)
  rm dist/*
  ;;

publish_pypi_test)
  (
    poetry config repositories.testpypi "https://test.pypi.org/legacy/"
    poetry config pypi-token.testpypi "$TESTPYPI_TOKEN"
    poetry publish --build -r testpypi
  )
  ;;

publish_pypi)
  (
    poetry config repositories.pypi "https://upload.pypi.org/legacy/"
    poetry config pypi-token.pypi "$PYPI_TOKEN"
    poetry publish --build -r pypi
  )
  ;;

publish_private)
  (
    poetry config repositories.priv_repo "${REPO_URL}"
    poetry config http-basic.priv_repo "${REPO_USER}" "${REPO_PASSWORD}"
    poetry publish -v -r priv_repo
  )
  ;;

install)
  poetry install
  ;;

update)
  poetry update
  ;;

*)
  echo "unknown command"
  ;;

esac
