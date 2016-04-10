#!/bin/python
from app import app
from flask import g

if __name__ == "__main__":
    from os import environ
    if 'WINGDB_ACTIVE' in environ:
        app.debug = False
        
    app.run(use_reloader=True)