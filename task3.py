from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
import string
from csv import reader

# spark-submit task1.py /user/ecc290/HW1data/parking-violations.csv /user/ecc290/HW1data/open-violations.csv
o_header = ['summons_number', 'plate',
            'license_type',
            'county', 'state', 'prescint',
            'issuing_agency', 'violation',
            'violation_status', 'issue_date',
            'violation_time', 'judgment_entry_date',
            'amount_due', 'payment_amount',
            'penalty_amount', 'fine_amount',
            'interest_amount', 'reduction_amount']

p_header = ['summons_number', 'issue_date', 'violation_code',
            'violation_county', 'violation_description',
            'violation_location', 'violation_precint',
            'violation_time', 'time_first_observed',
            'meter_number', 'issuer_code',
            'issuer_command', 'issuer_precinct',
            'issuing_agency', 'plate_id', 'plate_type',
            'registration_state', 'street_name',
            'vehicle_body_type', 'vehicle_color',
            'vehicle_make', 'vehicle_year']


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: bigram <open-violations-file>", file=sys.stderr)
        exit(-1)

    open_file = sys.argv[1]

    sc = SparkContext()
    lines_open = sc.textFile(open_file, 1).mapPartitions(lambda x: reader(x))

    open_v = lines_open.map(lambda x: (x[o_header.index('license_type')],
                                       float(x[o_header.index('amount_due')])))

    total_count = open_v.aggregateByKey((0.,0.),
                                        lambda x, y: (x[0] + y, x[1] + 1.),  # Seq function
                                        lambda a, b: (a[0] + b[0], a[1] + b[1]))  # Comb function


    out = total_count.mapValues(lambda v: '{0}, {1}'.format(v[0], v[0] / v[1]))
    out = out.map(lambda x: '{0}\t{1}, {2}'.format(x[0], x[1], x[2]))
    out.saveAsTextFile('task3.out')

    sc.stop()


# aggregateByKey example (https://stackoverflow.com/questions/29930110/calculating-the-averages-for-each-key-in-a-pairwise-k-v-rdd-in-spark-with-pyth)
#   First lambda expression for Within-Partition Reduction Step::
#   a: is a TUPLE that holds: (runningSum, runningCount).
#   b: is a SCALAR that holds the next Value
#
#   Second lambda expression for Cross-Partition Reduction Step::
#   a: is a TUPLE that holds: (runningSum, runningCount).
#   b: is a TUPLE that holds: (nextPartitionsSum, nextPartitionsCount).