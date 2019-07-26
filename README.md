# Stream-O-Matic
Code the medium article

1. Create and activate a Python Virtual Environment:

```bash
python3 -m pip install --user virtualenv
python3 -m virtualenv venv
source venv/bin/activate
```

2. Install SAM

```bash
pip install --upgrade aws-sam-cli
```

3. Install runtime requirements in your virtual enviroment :

```bash
pip install -r runtime/requirements.txt
```

4. Deploy the code

```bash
make all
```

The `Makefile` in each module just wraps the build/package/deploy for `sam`. 