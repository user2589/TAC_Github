#!/usr/bin/env python

"""
Script for gathering GitHub data
"""

import pandas as pd
import stscraper as scraper
from stutils import decorators as d
from stutils import mapreduce

import argparse
import logging

from stutils.decorators import cache_iterator

# import shutil
# import tempfile
# import json
# import ijson.backends.yajl2 as ijson
# from functools import wraps
#
#
# class cache_iterator(d.fs_cache):
#     """ A modification of fs_cache to handle large unstructured iterators
#         - e.g., a result of a GitHubAPI call
#     """
#     def __call__(self, func):
#         @wraps(func)
#         def wrapper(*args):
#             cache_fpath = self.get_cache_fname(
#                 func.__name__, *args, **{'extension': 'json'})
#
#             if not self.expired(cache_fpath):
#                 cache_fh = open(cache_fpath, 'rb')
#                 for item in ijson.items(cache_fh, "item"):
#                     yield item
#             else:
#                 # if iterator is not exhausted, the resulting file
#                 # will contain invalid JSON. So, we write to a tempfile
#                 # and rename when the iterator is exhausted
#                 cache_fh = tempfile.TemporaryFile()
#                 sep = "[\n"
#                 for item in func(*args):
#                     cache_fh.write(sep)
#                     sep = ",\n"
#                     cache_fh.write(json.dumps(item))
#                     yield item
#                 cache_fh.write("]")
#                 cache_fh.flush()
#                 # os.rename will fail if /tmp is mapped to a different device
#                 shutil.copyfileobj(cache_fh, cache_fpath)
#                 cache_fh.close()
#
#         return wrapper


fs_cache = d.typed_fs_cache('filtered')
cached_iterator = cache_iterator('raw')
gh_api = scraper.GitHubAPI()


get_raw_commits = cached_iterator(gh_api.repo_commits)
get_raw_issues = cached_iterator(gh_api.repo_issues)
get_raw_issue_comments = cached_iterator(gh_api.repo_issue_comments)
get_raw_issue_events = cached_iterator(gh_api.repo_issue_events)
get_raw_pulls = cached_iterator(gh_api.repo_pulls)


@fs_cache('commits')
def get_commits(repo):
    def gen():
        for commit in get_raw_commits(repo):
            yield scraper.json_map({
                'sha': 'sha',
                'author': 'author__login',
                'author_email': 'commit__author__email',
                'authored_at': 'commit__author__date',
                'committer': 'commit__committer__login',
                'committer_email': 'commit__committer__email',
                'committed_at': 'commit__committer__date',
                'comment_count': 'commit__comment_count',
                'message': 'commit__message'
            }, commit)

    return pd.DataFrame(gen()).set_index('sha')


@fs_cache('issues')
def get_issues(repo):
    def gen():
        for issue in get_raw_issues(repo):
            i = scraper.json_map({
                'number': 'number',
                'id': 'id',
                'state': 'state',
                'created_at': 'created_at',
                'updated_at': 'updated_at',
                'closed_at': 'closed_at',
                'user': 'user__login',
                'role': 'author_association',
                'reactions': 'reactions__total_count',
            }, issue)
            i['labels'] = ",".join(l['name'] for l in issue['labels'])
            yield i

    return pd.DataFrame(
        gen(), columns=('number', 'id', 'state', 'created_at', 'updated_at',
                        'closed_at', 'user', 'role', 'reactions')
    ).set_index('number')


@fs_cache('issue_comments', 2)
def get_issue_comments(repo):
    def gen():
        for comment in get_raw_issue_comments(repo):
            yield {
                'id': comment['id'],
                'issue_no': int(comment['issue_url'].rsplit("/", 1)[-1]),
                'body': comment['body'],
                'user': comment['user']['login'],
                'role': comment['author_association'],
                'created_at': comment['created_at'],
                'updated_at': comment['updated_at'],
                'reactions':
                    scraper.json_path(comment, 'reactions__total_count')
            }
    return pd.DataFrame(
            gen(), columns=('id', 'issue_no', 'body', 'user', 'role',
                            'created_at', 'updated_at', 'reactions')
        ).set_index(['issue_no', 'id'])


@fs_cache('issue_events', 2)
def get_issue_events(repo):
    # event: mentioned
    #       user = the one who was mentioned
    # event: closed|renamed
    #       user = the one who closed/renamed
    # event: subscribed
    #       doesn't make any sense
    def gen():
        for event in get_raw_issue_events(repo):
            yield scraper.json_map({
                'id': 'id',
                'issue_no': 'issue__number',
                'event': 'event',
                'user': 'actor__login',
                'created_at': 'created_at',
            }, event)

    return pd.DataFrame(
        gen(), columns=('id', 'issue_no', 'event', 'user', 'created_at')
    ).set_index(['issue_no', 'id'])


@fs_cache('pull_requests')
def get_pulls(repo):
    def gen():
        for pr in get_raw_pulls(repo):
            yield {
                'number': pr['number'],
                'title': pr['title'],
                'state': pr['state'],
                'user': pr['user']['login'],
                'created_at': pr['created_at'],
                'updated_at': pr['updated_at'],
                'closed_at': pr['closed_at'],
                'labels': ",".join(l['name'] for l in pr['labels']),
                'role': pr['author_association']
            }

    return pd.DataFrame(
        gen(), columns=('number', 'title', 'state', 'user', 'created_at',
                        'updated_at', 'closed_at', 'labels', 'role')
    ).set_index('number')


@cached_iterator
def get_raw_pull_commits(repo):
    for pr_no in get_pulls(repo).index:
        for commit in gh_api.pull_request_commits(repo, pr_no):
            commit['pr_no'] = pr_no
            yield commit


@fs_cache('pull_commits', 2)
def get_pull_commits(repo):
    def gen():
        for commit in get_raw_pull_commits(repo):
            yield scraper.json_map({
                'pr_no': 'pr_no',
                'sha': 'sha',
                'author': 'author__login',
                'author_email': 'commit__author__email',
                'authored_at': 'commit__author__date',
                'committer': 'commit__committer__login',
                'committer_email': 'commit__committer__email',
                'committed_at': 'commit__committer__date',
                'comment_count': 'commit__comment_count',
                'message': 'commit__message'
            }, commit)

    return pd.DataFrame(
        gen(), columns=('pr_no', 'sha', 'author', 'author_email', 'authored_at',
                        'committer', 'committer_email', 'committed_at',
                        'comment_count', 'message')
    ).set_index(['pr_no', 'sha'])


@cached_iterator
def get_raw_review_comments(repo):
    for pr_no in get_pulls(repo).index:
        for comment in gh_api.review_comments(repo, pr_no):
            comment['pr_no'] = pr_no
            yield comment


@fs_cache('pull_commits', 2)
def get_pull_review_comments(repo):
    def gen():
        for comment in get_raw_review_comments(repo):
            yield scraper.json_map({
                'pr_no': 'pr_no',
                'id': 'id',
                'user': 'user__login',
                'created_at': 'created_at',
                'updated_at': 'updated_at',
                'body': 'body',
                'path': 'path',
                'position': 'original_position',
                'role': 'author_association'
            }, comment)

    return pd.DataFrame(
        gen(), columns=('pr_no', 'id', 'user', 'created_at', 'updated_at',
                        'body', 'path', 'position', 'role')
    ).set_index(['pr_no', 'id'])


@fs_cache('labels')
def get_labels(repo):
    labels = gh_api.repo_labels(repo)
    return pd.Series(labels, index=labels)


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
        for metric, provider in metrics.items():
            provider(repo)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Collect cache for ")
    parser.add_argument('-i', '--input', default="-",
                        type=argparse.FileType('r'),
                        help='Input filename, "-" or skip for stdin')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help="Log progress to stderr")
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s %(message)s',
                        level=logging.INFO if args.verbose else logging.WARNING)

    repos = pd.read_csv('34k_dataset_1000_3_10.csv', index_col='repo')
    # repo_index = repo_index.iloc[0:2]  # for testing with 2 repos
    logging.info('Repos found: %d', len(repos))

    # mapreduce.map(collect_data, repos)
    for repository, row in repos.iterrows():
        collect_data(repository, row)
