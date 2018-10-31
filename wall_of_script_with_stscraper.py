
"""
Script for gathering GitHub data
"""

import pandas as pd
import stscraper as scraper
from stutils import decorators as d

import logging


fs_cache = d.typed_fs_cache('filtered')
cached_iterator = d.cache_iterator('raw')
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
            i['labels'] = ",".join(issue['labels'])
            yield i

    return pd.DataFrame(gen()).set_index('number')


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
    return pd.DataFrame(gen()).set_index(['issue_no', 'id'])


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

    return pd.DataFrame(gen()).set_index(['issue_no', 'id'])


@fs_cache('pull_requests')
def get_pulls(repo):
    def gen():
        for pr in get_raw_pulls(repo):
            yield {
                'number': pr['number'],
                'title': pr['title'],
                'state': pr['state'],
                'user': pr['user__login'],
                'created_at': pr['created_at'],
                'updated_at': pr['updated_at'],
                'closed_at': pr['closed_at'],
                'labels': ",".join(pr['labels']),
                'role': pr['author_association']
            }

    return pd.DataFrame(gen()).set_index('number')


@cached_iterator
def get_raw_pull_commits(repo):
    for pull in get_pulls(repo):
        for commit in gh_api.pull_request_commits(repo, pull['number']):
            commit['pr_no'] = pull['number']
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

    return pd.DataFrame(gen()).set_index(['pr_no', 'sha'])


@cached_iterator
def get_raw_review_comments(repo):
    for pull in get_pulls(repo):
        for comment in gh_api.review_comments(repo, pull['number']):
            comment['pr_no'] = pull['number']
            yield comment


@fs_cache('pull_commits', 2)
def get_pull_review_comments(repo, pr_id):
    for comment in get_raw_review_comments(repo):
        yield scraper.json_map({
            'pr_no': 'pr_no',
            'user': 'user__login',
            'created_at': 'created_at',
            'updated_at': 'updated_at',
            'body': 'body',
            'path': 'path',
            'position': 'original_position',
            'role': 'author_association'
        }, comment)


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


if __name__ == '__main__':

    repos = pd.read_csv('34k_dataset_1000_3_10.csv', index_col='repo')
    # repo_index = repo_index.iloc[0:2]  # for testing with 2 repos
    logging.info('Repos found: %d', len(repos))

    for repo in repos.index:
        logging.info('Processing %s', repo)
        if not gh_api.project_exists(repo):
            continue
        for metric, provider in metrics.items():
            _ = provider(repo)
