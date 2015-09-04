#!/usr/bin/env python
"""
    run_tests.py
    ~~~~~~~~~~~~

    Entry point for running scatter-async test suite.
"""


def main():
    import pytest

    pytest.main(['-s'])


if __name__ == '__main__':
    main()
