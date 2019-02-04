import datetime

from dataflows import Flow, load, dump_to_path, PackageWrapper, ResourceWrapper, printer
from datapackage import package



def set_format_home_price_ratio(package: PackageWrapper):
    package.pkg.descriptor['name'] = 'home-affordability'
    package.pkg.descriptor['title'] = 'London home affordability'
    # Change path and name for the resource:
    package.pkg.descriptor['resources'][0]['path'] = 'data/london-home-price-index.csv'
    package.pkg.descriptor['resources'][0]['name'] = 'london-home-index'

    yield package.pkg
    res_iter = iter(package)
    first: ResourceWrapper = next(res_iter)
    yield first.it
    yield from package

def date_formatter(date):
    if str(date).split('-')[0].isdigit():
        return str(date).split(' ')[0]
    elif len(str(date).split('-')[0]) == 4:
        return datetime.datetime.strptime(str(date), '%B-%Y').strftime('%Y-%m') + '-01'
    else:
        return datetime.datetime.strptime(str(date), '%b-%Y').strftime('%Y-%m') + '-01'

def tweak_row(row):
    for key, value in row.items():
        row[key] = str(value)
        if str(value) == '':
            row[key] = None
        if 'Month' in key:
            row[key] = date_formatter(value)
        if value == '.':
            row[key] = None



def filter_row_house_price_ratio(rows):
    for row in rows:
        if row['New Code'] != '':
            yield row



def house_price_ratio(link):
    Flow(
        load(link,
        format="xls",
        headers=[1, 1],
        fill_merged_cells=True,
        skip_rows=[2],
        sheet=2),
        filter_row_house_price_ratio,
        set_format_home_price_ratio,
        dump_to_path('data'),
        printer(num_rows=1)
    ).process()


house_price_ratio('https://data.london.gov.uk/download/ratio-house-prices-earnings-borough/122ea18a-cb44-466e-a314-e0c62a32529e/ratio-house-price-earnings-residence-based.xls')