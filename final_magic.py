#!/usr/bin/env python3

import argparse
import logging
import traceback

import pandas as pd
from stutils import mapreduce

import utils

START = '2018-01'
END = '2019-06'


def project_data(input_row):
    """

    Args:
         input_row (pd.Series: a row from filtered_2214_repos.csv

    Returns:
         pd.Series: fields described in TAC_TODO GDoc
    """
    result = input_row.copy()  # already includes repo_slug and core_size
    repo_slug = result['repo_slug']
    logging.info("%s: basic project metrics", repo_slug)
    repo = utils.get_repository(repo_slug)
    commits = utils.get_commits(repo_slug)
    result['project_size'] = len(commits)
    result['project_start'] = min(commits['committed_at'])
    result['project_end'] = max(commits['committed_at'])
    start_year = int(result['project_start'][:4])
    start_month = int(result['project_start'][5:7])
    end_year = int(result['project_end'][:4])
    end_month = int(result['project_end'][5:7])
    result['project_age'] = 12 * (end_year - start_year) + end_month - start_month

    logging.info("%s getting NPM metrics", repo_slug)
    package_names = utils.repo_package_names(repo_slug)
    if not package_names:
        result['error'] = "No matching packages"
        return result

    packages_info = {pkgname: utils.npm_info(pkgname)
                     for pkgname in package_names}
    main_package = max(
        package_names, key=lambda x: packages_info[x].get('npm_downloads'))
    result['main_package'] = main_package
    npm_info = packages_info[main_package]
    for key, value in npm_info.items():
        result[key] = value

    logging.info("%s: getting capacity metrics", repo_slug)
    contrib_matrix = utils.contribution_matrix(repo_slug, timeout=0)
    idx = pd.date_range(
        start=START, end=END, freq='M').to_series().dt.strftime('%Y-%m')
    cm = contrib_matrix.reindex(idx).fillna(0).astype(int)
    result['project_capacity'] = cm.sum(axis=1).mean()
    events = utils._role_events(repo_slug)
    events['date'] = events['date'].dt.strftime('%Y-%m')
    events = events[
        (events['role'] > utils.NOBODY)
        & (events['date'] >= START)
        & (events['date'] <= END)
    ]
    event_counts = events['event_type'].groupby(
        events['date']).count().rename(
        'event_count').reindex(idx).fillna(0).astype(int)
    result['events_count'] = event_counts.sum()
    result['fano_factor'] = event_counts.var() / max(event_counts.mean(), 0.001)

    logging.info("%s: modularity", repo_slug)
    modularity = utils.get_commit_modularity(repo)['louvain'].reindex(idx)
    result['modularity_start'] = modularity[0]
    result['modularity_end'] = modularity[-1]

    mti = utils.multiteaming_index(repo_slug)
    result['mtm_count'] = mti.reindex(idx).fillna(0).mean().mean()

    logging.info("%s: issues", repo_slug)
    issues = utils.get_issues(repo_slug)
    open_date = issues['created_at'].str[:7]
    close_date = issues['closed_at'].str[:7]
    result['open_issues_count_start'] = sum(
        (open_date < START) & ~(close_date < START))
    result['open_issues_count_during'] = sum(
        (open_date >= START) & (open_date <= END))

    result['closed_issues_count_during'] = sum(
        (close_date >= START) & (close_date <= END))
    return result.rename(repo_slug)


def process_data_w_exc(i, row):
    try:
        return project_data(row)
    except:
        return pd.Series({'error': traceback.format_exc()})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Preprocess Python imoprts data from WoC dataset')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help="Log progress to stderr")
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s %(message)s',
                        level=logging.INFO if args.verbose else logging.WARNING)

    df = pd.read_csv('filtered_2214_repos.csv')
    result = mapreduce.map(process_data_w_exc, df, num_workers=24)
    result.to_csv('processed_2214.csv')
