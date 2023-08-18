#!/usr/bin/env python3
import re
import glob
import urllib.parse
from collections import defaultdict
import pdb

seg_url_re = re.compile(r'^\[([\d:\-\s]+)\].*Failed to donwload segment (h?\d+).m4s from (https://[\w\-\.]+)')
seg_http_re = re.compile(r'\[([\d:\-\s]+)\].*Got HTTP \d+ when downloading segment (h?\d+).m4s from (https://[\w\-\.]+)')
seg_fail_re = re.compile(r'^\[([\d:\-\s]+)\].*Failed to download segment (\d+)')


def parse_log(fn):
    fails = defaultdict(lambda: {'url': {}, 'failed': False, 'ts': []})
    with open(fn, 'rt', encoding='utf-8') as f:
        while line := f.readline():
            if 'Got HTTP' in line:
                m = seg_http_re.search(line)
                ts, seg, url = m.groups()
                fails[seg]['url'][url] = fails[seg]['url'].get(url, 0) + 1
                fails[seg]['ts'].append((ts, url))
            elif 'Failed to do' in line:
                if m := seg_url_re.search(line):
                    ts, seg, url = m.groups()
                    fails[seg]['url'][url] = fails[seg]['url'].get(url, 0) + 1
                    fails[seg]['ts'].append((ts, url))
                elif m := seg_fail_re.search(line):
                    ts, seg = m.groups()
                    fails[seg]['ts'].append((ts, None))
                    fails[seg]['failed'] = True
                else:
                    print(line)
                    pdb.set_trace()
    return dict(fails)


def check_fails(fn):
    fails = parse_log(fn)
    repeat_stats = defaultdict(lambda: [0, 0])
    for seg, parsed in sorted(fails.items()):
        url = parsed['ts'][0][1]
        host = urllib.parse.urlparse(url).hostname.replace('.bilivideo.com', '')
        if url not in fails[seg]['url']:
            pdb.set_trace()
        if fails[seg]['url'][url] > 1:
            print(f'{seg} {"fail" if fails[seg]["failed"] else "    "} count={list(fails[seg]["url"].values())}')
            repeat_stats[host][0] += 1
        else:
            repeat_stats[host][1] += 1

    print()
    repeat_fail = repeat_success = 0
    for host, stats in repeat_stats.items():
        repeat_fail += stats[0]
        repeat_success += stats[1]
        print(f'{host: <20} {stats[0]: >3}/{stats[1]: >3}/{sum(stats): >3}')
    print('-'*40)
    print(f'{"Total": <20} {repeat_fail: >3}/{repeat_success: >3}/{repeat_fail + repeat_success: >3}')
    print()


files = sorted(glob.glob('rec/sisrec-*.log'))
for fn in files:
    print(fn)
    check_fails(fn)
    print()
