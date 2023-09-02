import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import time
import sys
import math
import importlib


my_settings = {
    'ddb_local': False,
    'ddb_region': 'us-west-2',
    'logging': 'summary',
    'dryrun': False,
    'max_items': 100
}

tape_file = 'init'

if len(sys.argv) > 1:
    tape_file = sys.argv[1]
    # my_settings['target_table'] = sys.argv[1]

try:
    tape = importlib.import_module(tape_file)

except ModuleNotFoundError as e:
    print('Module ' + tape_file + ' was not found')
    quit()

except:
    print("Error loading ")
    quit()


# module_name = input("Name of module? ")
# module = importlib.import_module(module_name)
# print(module.__doc__)

my_config = Config(
    region_name=my_settings['ddb_region'],
    connect_timeout=5, read_timeout=5,
    retries={'total_max_attempts': 3, 'mode': 'standard'},
)


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


# def generate_item(record_count, second, second_index):
#     pkval = math.floor((record_count-1) / my_settings['pk_count']) + 1
#     skval = ((record_count-1) % my_settings['pk_count']) + 1
#
#     return {
#             'PK': {'S': 'cust-' + str(pkval)},
#             'SK': {'S': 'event-' + str(skval)},
#             'balance': {'N': str(record_count*100)},
#             'payload': {'S': payload_data(3)}
#     }


def populate(dynamodb):
    latencylist = []
    record_count = 1
    wcu_this_second = 0
    wcu_total = 0

    start_time = time.time()
    item_start_second = round(start_time)
    second_index = 0
    previous_second = 0

    wcu_total = 0.0
    rcu_total = 0.0
    wcu_this_second = 0.0
    rcu_this_second = 0.0

    retry_attempts_this_second = 0
    retry_attempts_total = 0

    sample_record = tape.generate_item(1, 1)
    my_settings['target_table'] = sample_record['TableName']

    cap = table_capacity(my_settings['target_table'], dynamodb)
    ks = cap['KeySchema']

    gsi_summary = ''
    if 'GSI_count' in cap:
        gsi_summary = f', GSIs: {cap["GSI_count"]}'

    print(f'\nLoading table       : {bcolors.OKCYAN}{my_settings["target_table"]} ({cap["BillingMode"]}{gsi_summary}){bcolors.ENDC}')
    if 'WriteCapacityUnits' in cap:
        wcu_max_rate = cap["WriteCapacityUnits"]
        rcu_max_rate = cap["ReadCapacityUnits"]
        print(f'wcu/sec provisioned : {bcolors.OKCYAN}{wcu_max_rate}{bcolors.ENDC}')
        print(f'rcu/sec provisioned : {bcolors.OKCYAN}{rcu_max_rate}{bcolors.ENDC}')
        print()

    print(f'PKs : ' + str(my_settings['max_items']))

    # for i in range(my_settings['pk_count']):
    #     for j in range(my_settings['collection_size']):
    request = {'init': 1}

    while record_count <= my_settings['max_items'] and bool(request):

        request = tape.generate_item(record_count, 5)


        if previous_second != item_start_second:
            second_index += 1
            previous_second = item_start_second
            # print('      ' + str(capacity_units_this_second))
            wcu_this_second = 0.0
            rcu_this_second = 0.0

            if my_settings['logging'] == 'verbose':
                print()
                print()

        response = {}

        item_start_time = time.time()
        item_start_second = round(item_start_time)

        if bool(request):

            if my_settings['dryrun']:
                print(str(record_count) + '  ' + str(request))

            else:

                try:
                    # print('i ' + str(i) + ', j ' + str(j))
                    response = dynamodb.put_item(**request)

                except ClientError as ce:
                    error_code = ce.response['Error']['Code']
                    if error_code == 'ProvisionedThroughputExceededException':
                        print(error_code)

                finally:
                    item_end_time = time.time()
                    if my_settings['logging'] == 'verbose':
                        print('   ******   ' + str(item_start_time)[7:14] + ' ' + str(item_end_time)[7:14])

                    item_duration = round(round(item_end_time - item_start_time, 3) * 1000)
                    cu = response['ConsumedCapacity']['CapacityUnits']
                    ra = response['ResponseMetadata']['RetryAttempts']
                    if ra > 0:
                        print(bcolors.OKGREEN,'  * retries: ', ra, bcolors.ENDC)
                    retry_attempts_total += ra

                    wcu_total += cu
                    wcu_this_second += cu

                    latencylist.append({'second': str(item_start_second), 'latency': item_duration})
                    outputstring = str(record_count) + \
                        ' ' + str(second_index) + \
                        ' ' + str(cu) + \
                        ' ' + str(item_duration)
                    if my_settings['logging'] == 'verbose':
                        print(outputstring)

            record_count += 1


    print('\nItems               : ' + str(record_count-1))
    end_time = time.time()
    duration = end_time - start_time
    items_per_second = (record_count-1)/duration
    print('Duration in seconds : ' + str(round(duration,1)))
    print('Items per second    : ' + str(round(items_per_second,1)))


    if my_settings['logging'] in ['summary','verbose']:
        print(str(retry_attempts_total) + ' retries')


def payload_data(kbs):
    one_kb = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec lacus magna, consectetur vitae faucibus cursus, volutpat sit amet neque. Etiam eu dolor tempor, porttitor risus at, tristique justo. Mauris sollicitudin gravidae diam vitae auctor. Donec velit nunc, semper at varius vel, ornare ac leo. Mauris ac porta arcu. Nam ullamcorper ac ligula ut lobortis. Quisque in molestie velit, ac rutrum arcu. Mauris em lacus, malesuada id mattis a, hendrerit et nunc. Ut pretium congue nisl molestie ornare. Etiam eget leo finibus, eleifend velit sit amet, condimentum ipsum. Aliquam qui nisi quis orci maximus laoreet id vel mi. Phasellus suscipit, leo sed ullamcorper cursus, est nisi fermentum magna, vitae placerat dui nibh eu ipsum. Phasellus faucibus a ex et tempus. Nulla consequat ornare dui sagittis dictum. Curabitur scelerisque malesuada turpis ac auctor. Suspendisse sit amet sapien ac eros viverra tempor.  Draco Dormiens Nunquam Titillandus! Nulam convallis velit ornare ante viverra eget in. Etiam eget leo finibus."
    if isinstance(kbs, int):
        return one_kb * kbs
    else:
        parts = math.modf(kbs)
        return (one_kb * int(parts[1])) + one_kb[:int(parts[0] * 1024)]


def table_capacity(table, dynamodb):
    table_desc = dynamodb.describe_table(TableName=table)
    cap = {}
    if 'GlobalSecondaryIndexes' in table_desc['Table']:
        cap['GSI_count'] = len(table_desc['Table']['GlobalSecondaryIndexes'])
    if 'BillingModeSummary' in table_desc['Table']:
        cap['BillingMode'] = table_desc['Table']['BillingModeSummary']['BillingMode']
    else:
        cap['BillingMode'] = 'PROVISIONED'
        cap['WriteCapacityUnits'] = table_desc['Table']['ProvisionedThroughput']['WriteCapacityUnits']
        cap['ReadCapacityUnits'] = table_desc['Table']['ProvisionedThroughput']['ReadCapacityUnits']

    pktype = ''
    sktype = ''

    cap['KeySchema'] = table_desc['Table']['KeySchema']
    ads = table_desc['Table']['AttributeDefinitions']
    for ad in ads:
        if ad['AttributeName'] == cap['KeySchema'][0]['AttributeName']:
            pktype = ad['AttributeType']
        if ad['AttributeName'] == cap['KeySchema'][1]['AttributeName']:
            sktype = ad['AttributeType']

    cap['KeySchema'][0]['Type'] = pktype
    cap['KeySchema'][1]['Type'] = sktype

    return cap


def main(dynamodb=None):

    if not dynamodb:
        if my_settings['ddb_local']:
            dynamodb = boto3.client('dynamodb', config=my_config, endpoint_url='http://localhost:8000')
        else:
            dynamodb = boto3.client('dynamodb', config=my_config)

    populate(dynamodb)


if __name__ == "__main__":
    main()
