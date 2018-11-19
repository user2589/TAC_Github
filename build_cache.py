#!/usr/bin/env python

"""
Script for gathering GitHub data
"""

import pandas as pd

import argparse
import logging

from utils import *


raw_metrics = {
    'commits': get_raw_commits,
    'issues': get_raw_issues,
    'issue_comments': get_raw_issue_comments,
    'issue_events': get_raw_issue_events,
    'pulls': get_raw_pulls,
    'pull_commits': get_raw_pull_commits,
    'pull_review_comments': get_raw_review_comments,
}


metrics = {
    'commits': get_commits,
    'issues': get_issues,
    'issue_comments': get_issue_comments,
    'issue_events': get_issue_events,
    'pulls': get_pulls,
    'pull_commits': get_pull_commits,
    'pull_review_comments': get_pull_review_comments,
    'labels': get_labels
}


def collect_data(repo, row):
    logging.info('Processing %s', repo)
    if gh_api.project_exists(repo):
        for metric, provider in raw_metrics.items():
            provider(repo)
        for metric, provider in metrics.items():
            provider(repo)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Build data cache for TAC project")
    parser.add_argument('-i', '--input', default="-",
                        type=argparse.FileType('r'),
                        help='Input filename, "-" or skip for stdin')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help="Log progress to stderr")
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s %(message)s',
                        level=logging.INFO if args.verbose else logging.WARNING)

    repos = pd.read_csv(args.input, index_col='repo')
    # repo_index = repo_index.iloc[0:2]  # for testing with 2 repos
    logging.info('Repos found: %d', len(repos))

    from stutils import mapreduce
    mapreduce.map(collect_data, repos, num_workers=8)

    # for repository, row in repos.iterrows():
    #     collect_data(repository, row)
