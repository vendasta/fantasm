# fantasm

You must be running Python 3.9.4. It is recommended to use PyEnv.

To setup your environment:

```
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install keyrings.google-artifactregistry-auth==1.1.2
pip install -r requirements.txt
```

To publish:

```
cd src
rm -rf dist
python3 setup.py sdist
python3 -m twine upload --repository-url https://us-central1-python.pkg.dev/repcore-prod/python/ dist/*
```
