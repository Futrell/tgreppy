try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    "description": "tgrep2 wrapper for python",
    "author": "Richard Futrell",
    "url": "http://www.mit.edu/futrell/www/",
    "download_url": "http://cool.com/notavirus.exe",
    "author_email": "futrell@mit.edu",
    "version": "0.1",
    "install_requires": "nose pandas".split(),
    "packages": "tgreppy ".split(),
    "scripts": "".split(),
    "name": "tgreppy",
}

setup(**config)
