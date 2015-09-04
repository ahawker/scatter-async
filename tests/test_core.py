"""
    tests/test_core
    ~~~~~~~~~~~~~~~

    Contains tests for the :module: `tests.core.py` module.
"""

import pytest


# @pytest.fixture(session='module', params=[1, 0.5, 2])
# def interval(request):
#     return request.param
#
#
# @pytest.fixture(session='module')
# def spin_func(interval):
#     return interval % 2 == 0
#
#
# @pytest.fixture(session='module')
# def predicate(spin_func, interval):
#     return spin_func(interval) is True
#
#
# def test_spin_wait():
#     """
#
#     """