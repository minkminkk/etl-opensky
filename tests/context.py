import sys
import os

# Insert project folder to $PATH for easier imports
project_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(project_dir, "./src")))

from jobs import extract
from dags import *
