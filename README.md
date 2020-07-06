# tmc-summarizer
Process raw TMC data into more functional formats.

A live demo of this code is running within a Flask app here: [http://192.241.138.142](http://192.241.138.142)

![image info](./app/static/assets/images/app_screenshot.png)

## Development environment setup:

```bash
(base) $ conda create --name tmc_summarizer python=3.8
(base) $ conda activate tmc_summarizer
(tmc_summarizer) $ pip install -r requirements.txt
```

## Execute via Python, CLI, or GUI:

In all cases, you'll need the proper python environment activated.

```bash
(base) $ conda activate tmc_summarizer
(tmc_summarizer) $ 
```

### Python
```python
>>> from tmc_summarizer import write_summary_file
>>> write_summary_file("/my/raw/data/folder", "/my/output/folder")
```

### Command Line Interface

Use the ``TMC summarize`` command to kick off the script. You need to specify a path where the data can be found.

```bash
(tmc_summarizer) $ TMC summarize my/data/
```

### Graphical User Interface

Use the ``TMC gui`` command to visually select your data folder. 

```bash
(tmc_summarizer) $ TMC gui
```

## TODO
:white_check_mark: match functionality of prototype

:white_check_mark: add CLI hook

:white_check_mark: add GUI hook

:white_check_mark: write Flask-based web app

:black_square_button: write test suite

:black_square_button: test on Windows & Linux

## Questions:
``Light Vehicles`` tab has ``Peds in Crosswalk``, while the ``Heavy Vehicles`` tab has ``Bikes in Crosswalk``. Is this intentional, or mislabeled data?