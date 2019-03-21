#!/usr/bin/env python

"""
Script for gathering GitHub data
"""

import csv
import datetime
import os

import choicemodels
import git
import pandas as pd
import numpy as np
import stscraper
import stutils
from stutils import decorators as d
import stgithub


d.DEFAULT_EXPIRES = 3600*24*30*12

TAC_CACHE_PATH = os.path.join(
    stutils.get_config('ST_FS_CACHE_PATH'), 'TAC_repo_cache')
if not os.path.isdir(TAC_CACHE_PATH):
    os.mkdir(TAC_CACHE_PATH)


fs_cache = d.typed_fs_cache('filtered', expires=3600*24*30*12)
cached_iterator = d.cache_iterator('raw')
gh_api = stscraper.GitHubAPI()
scraper = stgithub.Scraper()

get_raw_commits = cached_iterator(gh_api.repo_commits)
get_raw_issues = cached_iterator(gh_api.repo_issues)
get_raw_issue_comments = cached_iterator(gh_api.repo_issue_comments)
get_raw_issue_events = cached_iterator(gh_api.repo_issue_events)
get_raw_pulls = cached_iterator(gh_api.repo_pulls)


@fs_cache('commits')
def get_commits(repo):
    def gen():
        for commit in get_raw_commits(repo):
            yield stscraper.json_map({
                'sha': 'sha',
                'author': 'author__login',
                'author_email': 'commit__author__email',
                'authored_at': 'commit__author__date',
                'committer': 'committer__login',
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
            i = stscraper.json_map({
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
                    stscraper.json_path(comment, 'reactions__total_count')
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
            yield stscraper.json_map({
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


@fs_cache('issues_and_prs')
def get_issues_and_prs(repo_slug):
    issues = get_issues(repo_slug)
    issues['is_pr'] = False
    prs = get_pulls(repo_slug)
    prs['is_pr'] = True
    columns = ['is_pr', 'state', 'created_at', 'closed_at', 'user', 'role']
    return pd.concat((issues[columns], prs[columns]), axis=0
                     ).sort_index(ascending=False)


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
            yield stscraper.json_map({
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
            yield stscraper.json_map({
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


@fs_cache('profiles', idx=2)
def get_profile(user, start=None, to=None):
    profile = pd.DataFrame(scraper.full_user_activity_timeline(user, start, to))
    profile['repo'] = profile['repo'].fillna('')
    return profile.set_index(['month', 'repo']).fillna(0).astype(int)


def get_assignments(repo_slug):
    events = get_issue_events(repo_slug).reset_index()
    issues = get_issues_and_prs(repo_slug)
    events['reporter'] = events['issue_no'].map(issues['user'])
    return events[(events['event'] == 'assigned')
                  & (events['reporter'] != events['user'])].copy()


def get_assignees(repo_slug):
    assignments = get_assignments(repo_slug)
    return assignments[['user', 'created_at']].groupby('user').min()


def get_collaborators(repo_slug):
    """Given repository slug, get people who committed directly to master.

    Returns:
        pd.Series: <github_login>: '%m-%d' formatted date of the earliest commit
    """
    cs = get_commits(repo_slug)
    repo = get_repository(repo_slug)
    fpc_sha = [c.hexsha for c in get_fp_chain(repo)]
    fpc = cs.loc[fpc_sha]
    return fpc[['committer', 'committed_at']].groupby('committer').min()[
        'committed_at'].rename('joined_date').str[:7]


def _get_choice_table(repo_slug):
    commits = get_commits(repo_slug)
    profiles = {}
    assignments = get_assignments(repo_slug)
    assignees = get_assignees(repo_slug).reset_index()

    for oid, assignment in assignments.iterrows():
        created_at = assignment['created_at']
        created_ts = pd.to_datetime(created_at)
        recently = (created_ts - datetime.timedelta(days=7)).strftime(
            "%Y-%m-%d")
        chosen_user = assignment['user']

        for aid, assignee in assignees.iterrows():
            candidate = assignee['user']

            if candidate not in profiles:
                profiles[candidate] = get_profile(candidate)

            n_commits = ((commits['author'] == candidate) & (
                        commits['authored_at'] < created_at)).sum()
            last_committed = commits.loc[(commits['author'] == candidate) & (
                        commits['authored_at'] < created_at),
                                         'authored_at'].max()
            if pd.isnull(last_committed):
                log_inactive = 8  # ~10 years
            else:
                log_inactive = np.log(
                    (created_ts - pd.to_datetime(last_committed)).days + 1)
            n_commits_recent = ((commits['author'] == candidate) & (
                        commits['authored_at'] > recently)).sum()
            try:
                lmi = profiles[candidate].loc[
                    (created_at[:7], repo_slug)].sum().sum()
            except KeyError:
                lmi = 0
            try:
                lme = profiles[candidate].loc[created_at[:7]].sum().sum()
            except KeyError:
                lme = 0
            lme -= lmi
            yield {
                'oid': oid,
                'issue_no': assignment['issue_no'],
                'aid': aid,
                'created_at': created_at,
                'unavailable': int(
                    assignee['created_at'] > assignment['created_at']),
                'log_n_commits': np.log(n_commits + 1),
                'log_inactive': log_inactive,
                'log_n_commits_recent': np.log(n_commits_recent + 1),
                'lmi': lmi,
                'lme': lme,
                'chosen': int(chosen_user == candidate)
            }


@fs_cache('choice_table')
def get_choice_table(repo_slug):
    return pd.DataFrame(_get_choice_table(repo_slug))


def get_mnl_params(repo_slug):
    m = choicemodels.MultinomialLogit(
        get_choice_table(repo_slug),
        'unavailable + log_n_commits + log_inactive + log_n_commits_recent '
        '+ lmi + lme - 1',
        'oid', 'chosen', alternative_id_col='aid'
    )
    return m.fit().fitted_parameters


def get_all_packages():
    reader = csv.DictReader(open('34k_dataset_1000_3_10.csv'))
    for row in reader:
        yield row['repo']


def core_contributors(project, threshold=0.8):
    commit_stats = get_commits(project)[
        ['author', 'authored_at']].groupby('author').count()[
        'authored_at'].sort_values(ascending=False).rename('commits')
    total = commit_stats.sum()
    shares = (commit_stats * 1.0 / total).cumsum()
    return list(shares[shares < threshold].index)


def get_repository(repo_slug):
    """ Get cloned Gitpython repository object, caching when possible """
    owner, proj_name = repo_slug.split("/")
    org_path = os.path.join(TAC_CACHE_PATH, owner)
    if not os.path.isdir(org_path):
        os.mkdir(org_path)
    repo_path = os.path.join(org_path, proj_name)
    if os.path.isdir(repo_path):
        repo = git.Repo(repo_path)
        repo.git.reset('--hard')
        repo.git.checkout('HEAD')
        return repo
    else:
        clone_url = 'git@github.com:%s.git' % repo_slug
        return git.Repo.clone_from(clone_url, repo_path)


def get_fp_chain(repo):
    """ Get chain of first-parent commits of the given repository """
    commits = []
    commit = repo.refs.master.commit
    while commit:
        commits.append(commit)
        commit = commit.parents and commit.parents[0]
    commits.reverse()
    return commits
