import click
import humanize
import re

from collections import defaultdict


@click.command()
@click.argument('file', type=click.File('r'))
@click.argument('option', type=click.Choice(['sort', 'aggregate']))
@click.option('--collection', default=None, help='Only inspect operations for a particular collection.')
# TODO allow sorting by any key that's in the "op" dict, not just size_out
def run(file, option, collection):

    print 'Gathering data'

    # Collect all the operation parts
    parts = []
    linenum = 1
    buffer = []
    for line in file.readlines():

        # If we encounter a header line, it's time to flush the buffer into
        # the list of db operation parts
        if '<<--' in line or '-->>' in line:
            part = part_from_buffer(buffer)
            if part:
                parts.append(part)
            buffer = []

        buffer.append(line)

    # Last part might still be in the buffer
    if buffer:
        part = part_from_buffer(buffer)
        if part:
            parts.append(part)

    # Merge the parts together
    ops = merge_parts(parts)

    # Filter by a collection if it was specified
    if collection is not None:
        ops = [op for op in ops if op['collection'] == collection]

    print 'Data gathered, analyzing...\n'

    if option == 'sort':
        ops = sorted(ops, key=lambda op: op['size_out'], reverse=True)
        for op in ops[:10]:  # top 10 ops
            print_op(op)
            print
    elif option == 'aggregate':
        data = aggregate_ops(ops)
        for d in data:
            print_aggregate_data(d)
            print


def print_op(op):
    """Print an op in a human-readable format"""
    print 'Collection: %s' % op['collection']
    print 'Size: %s (in) / %s (out)' % (
        humanize.naturalsize(op['size_in']),
        humanize.naturalsize(op['size_out'])
    )
    print 'Client: %s' % op['client']
    print 'Reply: %s' % op['data_out'][0].strip()[len('reply '):]
    print 'Query: %s' % ' '.join(d.strip() for d in op['data_in'])

def part_from_buffer(buffer):
    """
    Create a structured operation dict based on the buffer which is a list of
    lines that comprise a single operation.
    """
    # ignore empty buffers and killCursors ops
    if len(buffer) < 2 or 'killCursors' in buffer[1]:
        return

    # sample incoming request's header: 10.87.0.247:57754  -->> 10.0.3.197:27017 closeio.activity  170 bytes  id:afbc0f18   2948337432
    # sample outgoing response's header: 10.0.3.197:27017  <<--  10.223.128.98:57913   72336 bytes  id:14a63c90  346438800 - 2957084036
    header = re.sub('\s+', ' ', buffer[0]).split()

    # sanity check
    if header[1] not in ('-->>', '<<--'):
        raise Exception('this is not a header line')

    part = {
        'direction': 'in' if header[1] == '-->>' else 'out',
        'data': buffer[1:]
    }

    if part['direction'] == 'in':
        part['collection'] = header[3]
        part['size'] = int(header[4])
        part['client'] = header[0]
        part['server'] = header[2]
    else:
        part['size'] = int(header[3])
        part['client'] = header[2]
        part['server'] = header[0]

    return part

def merge_parts(parts):
    """
    Given a list containing parts of mongo operations (input and output),
    merge these parts together and return a list of complete db operations.

    Assumption: the output of a given db operation always appears in the list
    after the input, but not always *immediately* after. That's why we
    scan all the lines from input_time till infitnity to find the output.
    """
    ops = []
    for i, in_part in enumerate(parts):

        # skip the outputs
        if in_part['direction'] == 'out':
            continue

        found = False
        for out_part in parts[i+1:]:
            if out_part['client'] == in_part['client']:
                found = True
                break

        if found:
            ops.append({
                'client': in_part['client'],
                'server': in_part['server'],
                'size_in': in_part['size'],
                'size_out': out_part['size'],
                'collection': in_part['collection'],
                'data_in': in_part['data'],
                'data_out': out_part['data'],
            })
        else:
            print 'Match for an input not found:\n%s\n' % in_part

    return ops

def aggregate_ops(ops):
    """
    Agregate all the database operations and return information about the
    number of queries and the size of sent/received data per collection.
    """
    data = {}
    for op in ops:
        if op['collection'] not in data:
            data[op['collection']] = {
                'ops_cnt': 0,
                'size_in': 0,
                'size_out': 0
            }

        data[op['collection']]['ops_cnt'] += 1
        data[op['collection']]['size_in'] += op['size_in']
        data[op['collection']]['size_out'] += op['size_out']

    return sorted(data.items(), key=lambda d: d[1]['size_out'], reverse=True)

def print_aggregate_data(data):
    """Print aggregate data in a human-readable format"""
    collection, values = data
    print 'Collection: %s' % collection
    print 'Ops Count: %s' % values['ops_cnt']
    print 'Size: %s (in) / %s (out)' % (
        humanize.naturalsize(values['size_in']),
        humanize.naturalsize(values['size_out'])
    )


if __name__ == '__main__':
    run()

