from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
import string
from csv import reader

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
        print("Usage: bigram <parking-violations-file>", file=sys.stderr)
        exit(-1)

    parking_file = sys.argv[1]

    sc = SparkContext()
    lines_parking = sc.textFile(parking_file, 1).mapPartitions(lambda x: reader(x))

    parking_v = lines_parking.map(lambda x: ('{0}, {1}'.format(x[p_header.index('plate_id')],
                                                               x[p_header.index('registration_state')]),
                                             1))

    parking_v = parking_v.reduceByKey(lambda x, y: x + y)

    out = parking_v.takeOrdered(20, key=lambda x: -x[1])
    with open('task6.out', 'w') as f:
        for x in out:
            f.write('{0}\t{1}\n'.format(x[0], x[1]))

    sc.stop()
