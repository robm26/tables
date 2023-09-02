import math
import util

def hi(name):    # write Fibonacci series up to n
    print('hello ' + name)


def generate_item(request, second):
    target_table = 'table2'

    pk_count = 2
    collection_size = 3

    pkval = 'user-' + str((math.floor((request - 1) / collection_size)) + 1)
    skval = 'event-' + str(((request -1) % collection_size) + 1)

    if(request > (pk_count * collection_size)):
        return {}

    else:
        return {
            'TableName': target_table,
            'ReturnConsumedCapacity': 'TOTAL',
            'Item' : {
                'PK': {'S': pkval},
                'SK': {'S': skval},
                'Request': {'N': str(request)},
                'Second': {'N': str(second)},
                'City': {'S': 'Boston'},
                'Payload': {'S': util.payload_data(0.1)}
            }

        }

def assemble_item():

    return {}