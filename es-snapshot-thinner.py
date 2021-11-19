#!/usr/bin/env python

import datetime

from colorama import Fore, Back, Style
from elasticsearch import Elasticsearch

ES_HOST = 'http://127.0.0.1:9200/'
SNAPSHOT_REPO = 'ipfs_wasabi'
SNAPSHOT_PREFIX = "snapshot"


def name_to_date(name):
    """ Return datetime from snapshot name. """

    # Default format is snapshot_201231_0416

    date_part = name.split('_', 1)[1]
    date = datetime.datetime.strptime(date_part, '%y%m%d_%H%M')

    return date


def date_to_name(date):
    """ Return snapshot name for date. """

    fdate = date.strftime('%y%m%d_%H%M')

    return '{0}_{1}'.format(SNAPSHOT_PREFIX, fdate)


def find_closest(dates, date):
    """ Return the date from dates which lies closest to the requested date. """

    return min(dates, key=lambda d: abs((date - d).days))


def filter_dates(dates):
    """
    Return subset of dates such that we retain:
    - all snapshots in last 7 days
    - 3 additional snapshots spread maximally over the previous 28 days
    - Every 28 days (4 weeks) from the first
    """

    # Makes sure its sorted (in reverse, most recent first)
    # Of course, we could simply reverse the algorithm instead
    dates = sorted(dates, reverse=True)

    now = datetime.datetime.now()

    # This is the last of the dailies
    lastdaily = None
    lastmonthly = None
    earliest = dates[-1]

    print('Earliest:', earliest)
    print('Latest:', dates[0])

    # First iteration; split the ranks
    for d in dates:
        delta = now - d

        if delta < datetime.timedelta(days=7):
            # Within 7 dates, keep all
            print('Within 7 days:', d)
            lastdaily = d
            yield d

        elif delta < datetime.timedelta(days=28):
            # Find the last one within 28 days
            lastmonthly = d

        else:
            break

    ### Second iteration; max distance over last 31st days

    if lastmonthly:
        # If anything found in last month
        print('Last within the month:', lastmonthly)

        # Always keep the last monthly
        yield lastmonthly

        if lastdaily:
            # If anything within last week
            print('Last within the week:', lastdaily)

            idealspan = (lastmonthly - lastdaily)/3
            ideal1 = lastdaily + idealspan
            ideal2 = ideal1 + idealspan
            print('Ideal 1:', ideal1)
            print('Ideal 2:', ideal2)
            yield find_closest(dates, ideal1)
            yield find_closest(dates, ideal2)

    ### Third iteration; closes to start of month until last
    # Every 4 weeks until we're at the start
    idealmonthly = now - datetime.timedelta(weeks=4)

    while idealmonthly > earliest:
        print('Finding closest to:', idealmonthly)
        yield find_closest(dates, idealmonthly)

        # Iterate 4 weeks
        idealmonthly -= datetime.timedelta(weeks=4)

    # Always keep earliest
    yield earliest

def confirm():
    while True:
        confirm = input('[y]Yes or [n]No: ')
        if confirm in ('y', 'n'):
            return confirm == 'y'
        else:
            print("\n Invalid Option. Please enter [y] or [n].")


def main():
    # parser = argparse.ArgumentParser(description='Date-based snapshot thinner for Elasticsearch.')
    # parser.add_argument('urls', metavar='urls', type=str, nargs='+',
    #                     help='URLs of videos')
    # parser.add_argument('--ipfs-address', type=str, default='/dns/localhost/tcp/5001/http', help='IPFS HTTP API (multiaddr, default: \'/dns/localhost/tcp/5001/http\')')
    # parser.add_argument('--verbose', '-v', action='count', default=0)

    # args = parser.parse_args()

    # setupLogging(args.verbose)

    es = Elasticsearch([ES_HOST], timeout=1200)

    print('Checking for running snapshot delete tasks...')
    res = es.tasks.list(actions='cluster:admin/snapshot/delete', group_by='parents')
    if res['tasks'] != {}:
        print(res)
        print('Found running snapshot task, refusing to operate...')
        exit(-1)

    print('Verifying repository...')
    es.snapshot.verify_repository(SNAPSHOT_REPO)

    print('Listing snapshots...')
    res = es.snapshot.get(SNAPSHOT_REPO, '_all', verbose=False)

    snapshots = res['snapshots']

    # Filter sucessful
    success_snapshots = filter(lambda s: s['state'] == 'SUCCESS', snapshots)
    success_names = set(map(lambda s: s['snapshot'], success_snapshots))

    failed_snapshots = filter(lambda s: s['state'] != 'SUCCESS', snapshots)
    failed_names = set(map(lambda s: s['snapshot'], failed_snapshots))
    print('Found failed snapshots: ', ', '.join(failed_names))

    dates = set(map(name_to_date, success_names))
    keepers = set(filter_dates(dates))
    keep_names = set(map(date_to_name, keepers))

    assert(keep_names)
    print(keep_names)

    delete_names = (success_names - keep_names) | failed_names

    if delete_names:
        # Nice coloured output
        def colour_names(n):
            if n in keep_names:
                return Fore.GREEN + n + Style.RESET_ALL

            if n in delete_names:
                return Fore.RED + n + Style.RESET_ALL

            assert False, "Name should either be in keepers or deleters."

        print('\nProposing to keep the items in green and to delete the items in red:')
        all_names = sorted(delete_names | keep_names)

        assert len(all_names) == len(delete_names) + len(keep_names)

        display_names = list(map(colour_names, all_names))
        for n in display_names:
            print(n)

        def delete_snapshots(l):
            print('Deleting snapshots:', ', '.join(l))

            # TODO: Catch exceptions
            # TODO: Increase timeout
            # es.snapshot.delete(SNAPSHOT_REPO, l)

        print('\nDoes this seem reasonable? (y/n)')
        if confirm():
            # TODO: FUUUU delete_names contains *all* repositories!!!!
            # delete_snapshots(sorted(delete_names))

            print('Cleaning up repository...')
            es.snapshot.cleanup_repository(SNAPSHOT_REPO)
        else:
            print('kbye...')
    else:
        print('No snapshots found to delete... Goodbye!')


if __name__ == "__main__":
    main()
