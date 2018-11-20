#!/usr/bin/env python

"""
Script for gathering GitHub data
"""

from stutils.mapreduce import ThreadPool
from stscraper import RepoDoesNotExist

import argparse
import csv
import logging

from utils import *


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Build data cache for TAC project")
    parser.add_argument('-i', '--input', default="-",
                        type=argparse.FileType('r'),
                        help='Input filename, "-" or skip for stdin')
    parser.add_argument('-o', '--output', default="-",
                        type=argparse.FileType('wb'),
                        help='Output filename, "-" or skip for stdin')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help="Log progress to stderr")
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s %(message)s',
                        level=logging.INFO if args.verbose else logging.WARNING)

    reader = csv.DictReader(args.input)

    columns = ('repo', 'started_at', 'commits', 'committers', 'issues',
               'reporters', 'issue_comments', 'pull_requests',
               'review_comments', 'events', 'labels')
    writer = csv.DictWriter(args.output, fieldnames=columns)
    writer.writeheader()

    def get_stats(repo):
        try:
            commits = get_commits(repo)
        except RepoDoesNotExist:
            return None
        issues = get_issues(repo)
        return {
            'repo': repo,
            'started_at': min(commits['authored_at']),
            'commits': len(commits),
            'committers': len(commits['author'].unique()),
            'issues': len(issues),
            'reporters': len(issues['user'].unique()),
            'issue_comments': len(get_issue_comments(repo)),
            'pull_requests': len(get_pulls(repo)),
            'review_comments': len(get_pull_review_comments(repo)),
            'events': len(get_issue_events(repo)),
            'labels': len(get_labels(repo))
        }

    def output(stats):
        if stats:
            writer.writerow(stats)

    tp = ThreadPool(n_workers=8)

    for row in reader:
        tp.submit(get_stats, row['repo'], callback=output)

    tp.shutdown()
