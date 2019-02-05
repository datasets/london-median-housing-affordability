import datetime

from dataflows import Flow, load, dump_to_path, PackageWrapper, ResourceWrapper, printer, unpivot
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


unpivot_fields = [
    {'name': '2002', 'keys': {'Value': '2002', 'Year': '2002'}},
    {'name': '2003', 'keys': {'Value': '2003', 'Year': '2003'}},
    {'name': '2004', 'keys': {'Value': '2004', 'Year': '2004'}},
    {'name': '2005', 'keys': {'Value': '2005', 'Year': '2005'}},
    {'name': '2006', 'keys': {'Value': '2006', 'Year': '2006'}},
    {'name': '2007', 'keys': {'Value': '2007', 'Year': '2007'}},
    {'name': '2008', 'keys': {'Value': '2008', 'Year': '2008'}},
    {'name': '2009', 'keys': {'Value': '2009', 'Year': '2009'}},
    {'name': '2010', 'keys': {'Value': '2010', 'Year': '2010'}},
    {'name': '2011', 'keys': {'Value': '2011', 'Year': '2011'}},
    {'name': '2012', 'keys': {'Value': '2012', 'Year': '2012'}},
    {'name': '2013', 'keys': {'Value': '2013', 'Year': '2013'}},
    {'name': '2014', 'keys': {'Value': '2014', 'Year': '2014'}},
    {'name': '2015', 'keys': {'Value': '2015', 'Year': '2015'}},
    {'name': '2016', 'keys': {'Value': '2016', 'Year': '2016'}},
    {'name': '2017', 'keys': {'Value': '2017', 'Year': '2017'}}
]
extra_keys = [
    {'name': 'Year', 'type': 'any'}
]
extra_value = {'name': 'Value', 'type': 'any'}


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
        unpivot(unpivot_fields, extra_keys, extra_value),
        dump_to_path('.'),
        printer(num_rows=1)
    ).process()

house_price_ratio('https://data.london.gov.uk/download/ratio-house-prices-earnings-borough/122ea18a-cb44-466e-a314-e0c62a32529e/ratio-house-price-earnings-residence-based.xls')
