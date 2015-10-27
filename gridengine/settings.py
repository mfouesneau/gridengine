"""
Django-Style configuration file for GridEngine
"""
import os
import sys


# ----------------------------------------------------------------------------
# General
# ----------------------------------------------------------------------------
path = os.path.dirname(os.path.abspath(__file__))
PYTHONPATH = ':'.join(sys.path)
WRAPPER = os.path.join(path, 'wrapper.sh')
TEMPDIR = os.path.join(os.getcwd(), 'gridengine.log')
CURDIR = os.getcwd()


# ----------------------------------------------------------------------------
# Default Resources
# ----------------------------------------------------------------------------
try:
    import drmaa
    if drmaa.Session.drmsInfo == 'PBS Professional':
        # PBS/Torque Scheduler
        DEFAULT_RESOURCES = {}
    else:
        # Sun GridEngine Scheduler
        DEFAULT_RESOURCES = {}
except:
    # Process Scheduler
    DEFAULT_RESOURCES = {}
