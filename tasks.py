import os

from invoke import task


@task()
def test(c):
    current_pythonpath = os.environ.get('PYTHONPATH', '')
    new_pythonpath = f"src:test:{current_pythonpath}"
    c.run("python -m unittest discover", env={'PYTHONPATH': new_pythonpath})
