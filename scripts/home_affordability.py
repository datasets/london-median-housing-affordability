import datetime

from dataflows import Flow, load, dump_to_path, PackageWrapper, ResourceWrapper, printer, unpivot



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
    """
    Format date strings into a consistent format (YYYY-MM-DD).

    Handles various date formats:
    - ISO format dates (YYYY-MM-DD)
    - Full month name format (January-2023)
    - Abbreviated month name format (Jan-2023)

    Args:
        date: The date string to format

    Returns:
        Formatted date string in YYYY-MM-DD format
    """
    date_str = str(date)

    # Already in ISO format (YYYY-MM-DD)
    if date_str.split('-')[0].isdigit() and len(date_str.split('-')[0]) == 4:
        # Just return the date part if there's a time component
        return date_str.split(' ')[0]

    try:
        # Full month name format (e.g., January-2023)
        if len(date_str.split('-')[0]) > 3:
            return datetime.datetime.strptime(date_str, '%B-%Y').strftime('%Y-%m') + '-01'
        # Abbreviated month name format (e.g., Jan-2023)
        else:
            return datetime.datetime.strptime(date_str, '%b-%Y').strftime('%Y-%m') + '-01'
    except ValueError:
        # If parsing fails, return the original string
        return date_str

def tweak_row(row):
    """
    Clean and format row data.

    - Converts all values to strings
    - Handles empty values and special characters
    - Formats date fields

    Args:
        row: Dictionary containing row data
    """
    for key, value in row.items():
        # Convert to string first
        str_value = str(value)

        # Handle empty values and special characters
        if str_value == '' or str_value == '.' or str_value.lower() == 'nan':
            row[key] = None
        # Format date fields
        elif 'Month' in key:
            row[key] = date_formatter(value)
        # Keep the string value for other fields
        else:
            row[key] = str_value



def filter_row_house_price_ratio(rows):
    """
    Filter rows based on the 'New Code' field.

    Only rows with a non-empty 'New Code' field are included.

    Args:
        rows: Iterator of row dictionaries

    Yields:
        Filtered rows
    """
    for row in rows:
        # Check if 'New Code' exists and is not empty
        if 'New Code' in row and row['New Code'] != '':
            yield row


# Generate unpivot fields for years from 2002 up to current year + 5 years
def generate_unpivot_fields():
    """
    Generate unpivot fields dynamically for years from 2002 up to current year + 5 years.

    This ensures the script will work with future data without requiring manual updates.

    Returns:
        List of unpivot field definitions
    """
    current_year = datetime.datetime.now().year
    # Generate fields for years from 2002 up to 5 years in the future
    fields = []
    for year in range(2002, current_year + 6):  # +6 to include the current year + 5 future years
        year_str = str(year)
        fields.append({
            'name': year_str,
            'keys': {'Value': year_str, 'Year': year_str}
        })
    return fields

# Generate the unpivot_fields dynamically
unpivot_fields = generate_unpivot_fields()

extra_keys = [
    {'name': 'Year', 'type': 'any'}
]
extra_value = {'name': 'Value', 'type': 'any'}

def house_price_ratio(link):
    Flow(
        load(link,
        format="xlsx",
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
