#!/usr/bin/env bash

_VENVSETUP_BASE_PYTHON="${PYTHON:-python}"
_VENVSETUP_DIR="${VIRTUAL_ENV:-${PWD}/.venv}"
_MY_DIR="$( cd "$( dirname "${0}" )" && pwd )"

_venvsetup() {
    (
        set -e

        if [ ! -e "${_VENVSETUP_DIR}/bin/python" ] \
                || [ ! -e "${_VENVSETUP_DIR}/bin/pip" ] ; then
            if ! "${_VENVSETUP_BASE_PYTHON}" -c 'import sys ; sys.exit(0 if sys.version_info >= (3, 7) else 1)' ; then
                echo 1>&2 "${0}: Python 3.7+ is required; but $( bash -c "which ${_VENVSETUP_BASE_PYTHON}" ) is:"
                "${_VENVSETUP_BASE_PYTHON}" 2>&1 --version --version \
                    | sed 1>&2 '-es%^%    %'
                echo 1>&2 "${0}: Override with:"
                echo 1>&2 "    PYTHON=/path/to/python ${0}"

                return 1
            fi

            if "${_VENVSETUP_BASE_PYTHON}" >/dev/null 2>&1 -m virtualenv --version ; then
                "${_VENVSETUP_BASE_PYTHON}" -m virtualenv "${_VENVSETUP_DIR}"
            elif "${_VENVSETUP_BASE_PYTHON}" >/dev/null 2>&1 -m venv -h ; then
                "${_VENVSETUP_BASE_PYTHON}" -m venv "${_VENVSETUP_DIR}"
            else
                echo 1>&2 "${0}: can't find suitable virtualenv; giving up"

                return 1
            fi
        fi

        "${_VENVSETUP_DIR}/bin/pip" install --upgrade tox
    ) \
            || return "${?}"
}

_venvsetup \
    && . "${_VENVSETUP_DIR}/bin/activate"
