import logging
import os
import site
import sys

approot = os.path.dirname(__file__)
sys.path.append(os.path.join(approot, 'common'))
sys.path.append(os.path.join(approot, 'lib'))

def webapp_add_wsgi_middleware(app):
  from google.appengine.ext.appstats import recording
  app = recording.appstats_wsgi_middleware(app)
  return app
