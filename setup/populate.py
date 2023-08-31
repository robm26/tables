import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import time
import sys
import math


ddb_local = False
ddb_region = 'us-west-2'

target_table = 'table2'

pk_count = 10
collection_size = 10

if len(sys.argv) > 1:
    target_table = sys.argv[1]

my_config = Config(
    region_name=ddb_region,
    connect_timeout=5, read_timeout=5,
    retries={'total_max_attempts': 3, 'mode': 'standard'}
)

def generate_item(record_count, second, second_index):
    pkval = math.floor((record_count-1) / pk_count) + 1
    skval = ((record_count-1) % pk_count) + 1

    return {
            'PK': {'S': 'cust-' + str(pkval)},
            'SK': {'S': 'event-' + str(skval)},
            'balance': {'N': str(record_count*100)}
    }

def populate(table, dynamodb):
    latencylist = []
    record_count = 1

    start_time = time.time()
    item_start_second = round(start_time)
    second_index = 0
    previous_second = 0
    capacity_units_total = 0.0
    capacity_units_this_second = 0.0

    for i in range(pk_count):
        for j in range(collection_size):

            request = {
                'TableName': target_table,
                'ReturnConsumedCapacity': 'TOTAL',
                'Item': generate_item(record_count, 1, 1)
            }

            item_start_time = time.time()
            item_start_second = round(item_start_time)

            if previous_second != item_start_second:
                second_index += 1
                previous_second = item_start_second
                # print('      ' + str(capacity_units_this_second))
                capacity_units_this_second = 0.0

                print()
                print()

            response = {}

            try:
                # print('i ' + str(i) + ', j ' + str(j))
                response = dynamodb.put_item(**request)

            except ClientError as ce:
                error_code = ce.response['Error']['Code']
                if error_code == 'ProvisionedThroughputExceededException':
                    print(error_code)

            finally:
                item_end_time = time.time()
                print('   ******   ' + str(item_start_time)[7:14] + ' ' + str(item_end_time)[7:14])
                item_duration = round(round(item_end_time - item_start_time, 3) * 1000)
                cu = response['ConsumedCapacity']['CapacityUnits']

                capacity_units_total += cu
                capacity_units_this_second += cu
                latencylist.append({'second': str(item_start_second), 'latency': item_duration})
                outputstring = str(record_count) + \
                    ' ' + str(second_index) + \
                    ' ' + str(cu) + \
                    ' ' + str(item_duration)

                print(outputstring)
                record_count += 1


def main(dynamodb=None):

    if not dynamodb:
        if ddb_local:
            dynamodb = boto3.client('dynamodb', config=my_config, endpoint_url='http://localhost:8000')
        else:
            dynamodb = boto3.client('dynamodb', config=my_config)

    populate(target_table, dynamodb)


if __name__ == "__main__":
    main()
