"""
For this task, I was asked to write a streaming function that would read a csv
with sales data and output the number of unique customers who bought each
product and the total reveue brought in per product.

To run this script from the command line, use the following format:

python streaming.py <INPUT_CSV> <OUTPUT_CSV>

The 'sale.csv' file in the 'data' directory of this repo can be used as the
input csv. If you have a larger csv file you'd like to use to test against
this script, it will just need to have customer ID numbers in the first
column, product ID numbers in the 4th column, and item costs in the 5th column.
(This script is designed to be able to handle much larger data sets than the
'sale.csv' file.)
"""

import sys
import csv
from collections import defaultdict

if __name__=='__main__':
    def streamer(filename):
        with open(sys.argv[1], 'r') as fi:
            reader = csv.reader(fi)
            next(reader, None) # skip the header
            for row in reader:
                prod_id = row[3]
                cost = float(row[4])
                cust_id = row[0]
                yield prod_id, cost, cust_id

    with open(sys.argv[2], 'w') as fo:
        writer = csv.writer(fo)
        sales = {}
        customers = defaultdict(set)

        for pid, c, cid in streamer('sale.csv'):
            sales[pid] = round(sales.get(pid,0) + c, 2)
            customers[pid].add(cid)

        for key, value in customers.items():
            customers[key] = len(customers[key])

        dicts = sales, customers
        writer.writerow(['Product ID', 'Total Revenue', 'Customers'])

        for key in sorted(sales):
            writer.writerow([key] + [d[key] for d in dicts])
