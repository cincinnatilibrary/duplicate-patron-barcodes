#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import csv
from datetime import datetime, timedelta
import os

from sierra_db import execute_query_yield_rows, get_cursor
from chpl_email import send_email

from jinja2 import Environment, FileSystemLoader

# Set up the Jinja2 environment and load the template
env = Environment(loader=FileSystemLoader('.'))
template = env.get_template('template.html')

try:
    with open('config.json') as f:
        config = json.load(f)
        dsn = config['dsn']
except Exception as e:
    print(e)
    
directory = "./output"

# Check if the directory exists
if not os.path.exists(directory):
    # If the directory doesn't exist, create it
    os.makedirs(directory)
    
# Get the current date and format it as a string
date_str = datetime.now().strftime("%Y-%m-%d")


# Now you can use this directory to save your file
filename = f"duplicate-patron-barcodes_{date_str}.csv"
filepath = os.path.join(directory, filename)


# In[2]:


sql = """\
select 
    'Barcode' as identifier_type,
    v.field_content as identifier,
    json_agg(rm.record_type_code || rm.record_num || 'a') as patron_record_id_array 
from 
    sierra_view.varfield v 
    join sierra_view.record_metadata rm on (
        rm.id = v.record_id
        and rm.record_type_code = 'p'
        and rm.campus_code = ''
    )
where 
    v.varfield_type_code = 'b'     -- barcode
group by 
    v.field_content 
having
    count(distinct rm.id) > 1
union all
select 
    'Alternate ID' as identifier_type,
    v.field_content as identifier,
    json_agg(rm.record_type_code || rm.record_num || 'a') as patron_record_id_array
from 
    sierra_view.varfield v 
    join sierra_view.record_metadata rm on (
        rm.id = v.record_id
        and rm.record_type_code = 'p'
        and rm.campus_code = ''
    )
where 
    v.varfield_type_code = 'v'     -- alt id
group by 
    v.field_content 
having
    count(distinct rm.id) > 1
"""

with get_cursor(dsn=dsn) as cursor:
    rows = execute_query_yield_rows(cursor, sql, None)
    
    with open(filepath, 'w') as f:
        writer = csv.writer(f)
        columns = next(rows)
        writer.writerow(columns)
        
        # Initialize an empty list to store the data for the template
        data = []
        
        for i, row in enumerate(rows):
            writer.writerow(row)
            
            # Add this row's data to the list for the template
            data.append(dict(zip(columns, row)))
            
            if i % 1000 == 0:
                print('.', end='')
        print(f'.done ({i+1})')


# In[3]:


# Render the template with the data
html = template.render(results=data)

# Now you can use the HTML string however you like, e.g., write it to a file or send it in an email
with open('report.html', 'w') as f:
    f.write(html)

html = template.render(results=data)


# In[4]:


send_email(
    smtp_username=config['smtp_username'], 
    smtp_password=config['smtp_password'], 
    subject="Duplicate Identifier for Patron Records Report", 
    message="See attached.",
    html=html,
    from_addr="ray.voelker@chpl.org", 
    to_addr=config['send_list'], 
    files=[filepath, 'report.html']
)

