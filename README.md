<a className="gh-badge" href="https://datahub.io/core/london-median-housing-affordability"><img src="https://badgen.net/badge/icon/View%20on%20datahub.io/orange?icon=https://datahub.io/datahub-cube-badge-icon.svg&label&scale=1.25" alt="badge" /></a>

London Median Housing Affordability Dataset for DataHub

## Data

Data was downloaded from https://www.gov.uk/government/statistics/house-price-indexes-for-england-and-wales-quarterly-data-2016-to-present
and processed using scripts provided here.


## Preparation

Process is recorded and automated in python script:

```
python -m venv .venv
source .venv/bin/activate
pip install -r scripts/requirements.txt
python scripts/process.py
```

## Automation

Up-to-date (auto-updates every month) london median housing affordability dataset could be found on the datahub.io:
https://datahub.io/core/london-median-housing-affordability

## License

This Data Package is made available under the Public Domain Dedication and License v1.0 whose full text can be found at: http://www.opendatacommons.org/licenses/pddl/1.0/
