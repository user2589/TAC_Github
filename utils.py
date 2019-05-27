#!/usr/bin/env python

"""
Script for gathering GitHub data
"""

import csv
import datetime
import json
import logging
import os

# import choicemodels
import git
import pandas as pd
import networkit as nk
import numpy as np
import stscraper
import stutils
from stutils import decorators as d
from stutils import mapreduce
# import stgithub
from stecosystems import npm

import ghtorrent

d.DEFAULT_EXPIRES = 3600*24*30*12

TAC_CACHE_PATH = os.path.join(
    stutils.get_config('ST_FS_CACHE_PATH'), 'TAC_repo_cache')
if not os.path.isdir(TAC_CACHE_PATH):
    os.mkdir(TAC_CACHE_PATH)

ONE_YEAR = expires=3600 * 24 * 30 * 12
fs_cache_filtered = d.typed_fs_cache('filtered', expires=ONE_YEAR)
fs_cache_aggregated = d.typed_fs_cache('aggregated', expires=ONE_YEAR)
cached_iterator = d.cache_iterator('raw')
gh_api = stscraper.GitHubAPI()
scraper = stgithub.Scraper()

get_raw_commits = cached_iterator(gh_api.repo_commits)
get_raw_issues = cached_iterator(gh_api.repo_issues)
get_raw_issue_comments = cached_iterator(gh_api.repo_issue_comments)
get_raw_issue_events = cached_iterator(gh_api.repo_issue_events)
get_raw_pulls = cached_iterator(gh_api.repo_pulls)


# =====================================
# List projects
# =====================================

def all_projects():
    # type: () -> pd.Series
    """ Get list of all available projects """
    return pd.read_csv('34k_dataset_1000_3_10.csv', usecols=['repo'], squeeze=True)


def final_repos():
    # type: () -> pd.Series
    """ Only repos satisfying final criteria """
    return pd.read_csv('slugs_shortlist.csv', index_col=0, squeeze=True)


# =====================================
# Individual project metrics
# =====================================

@fs_cache_filtered('commits')
def get_commits(repo_slug):
    def gen():
        for commit in get_raw_commits(repo_slug):
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


@fs_cache_filtered('issues')
def get_issues(repo_slug):
    def gen():
        for issue in get_raw_issues(repo_slug):
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


@fs_cache_filtered('issue_comments', 2)
def get_issue_comments(repo_slug):
    def gen():
        for comment in get_raw_issue_comments(repo_slug):
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


@fs_cache_filtered('issue_events')
def get_issue_events(repo_slug):
    # event: mentioned
    #       user = the one who was mentioned
    # event: closed|renamed
    #       user = the one who closed/renamed
    # event: subscribed
    #       doesn't make any sense
    def gen():
        for event in get_raw_issue_events(repo_slug):
            yield stscraper.json_map({
                'id': 'id',
                'issue_no': 'issue__number',
                'event': 'event',
                'user': 'actor__login',
                'created_at': 'created_at',
            }, event)

    return pd.DataFrame(
        gen(), columns=('id', 'issue_no', 'event', 'user', 'created_at')
    ).set_index('issue_no')


@fs_cache_filtered('pull_requests')
def get_pulls(repo_slug):
    def gen():
        for pr in get_raw_pulls(repo_slug):
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


@fs_cache_filtered('issues_and_prs')
def get_issues_and_prs(repo_slug):
    issues = get_issues(repo_slug)
    issues['is_pr'] = False
    prs = get_pulls(repo_slug)
    prs['is_pr'] = True
    columns = ['is_pr', 'state', 'created_at', 'closed_at', 'user', 'role']
    return pd.concat((issues[columns], prs[columns]), axis=0
                     ).sort_index(ascending=False)


@cached_iterator
def get_raw_pull_commits(repo_slug):
    for pr_no in get_pulls(repo_slug).index:
        for commit in gh_api.pull_request_commits(repo_slug, pr_no):
            commit['pr_no'] = pr_no
            yield commit


@fs_cache_filtered('pull_commits', 2)
def get_pull_commits(repo_slug):
    def gen():
        for commit in get_raw_pull_commits(repo_slug):
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
def get_raw_review_comments(repo_slug):
    for pr_no in get_pulls(repo_slug).index:
        for comment in gh_api.review_comments(repo_slug, pr_no):
            comment['pr_no'] = pr_no
            yield comment


@fs_cache_filtered('pull_commits')
def get_pull_review_comments(repo_slug):
    def gen():
        for comment in get_raw_review_comments(repo_slug):
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


@fs_cache_filtered('labels')
def get_labels(repo_slug):
    labels = gh_api.repo_labels(repo_slug)
    return pd.Series(labels, index=labels)


# =====================================
# User data
# =====================================


@fs_cache_filtered('profiles', idx=2)
def get_profile(user, start=None, to=None):
    profile = pd.DataFrame(scraper.full_user_activity_timeline(user, start, to))
    profile['repo'] = profile['repo'].fillna('')
    return profile.set_index(['month', 'repo']).fillna(0).astype(int)


# =====================================
# Contributors
# =====================================


def get_assignments(repo_slug):
    events = get_issue_events(repo_slug).reset_index()
    issues = get_issues_and_prs(repo_slug)
    events['reporter'] = events['issue_no'].map(issues['user'])
    return events[(events['event'] == 'assigned')
                  & (events['reporter'] != events['user'])].copy()


def get_assignees(repo_slug):
    assignments = get_assignments(repo_slug)
    return assignments[['user', 'created_at']].groupby('user').min()


def get_committers(repo_slug):
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
    commit = repo.head.commit
    while commit:
        commits.append(commit)
        commit = commit.parents and commit.parents[0]
    commits.reverse()
    return commits


GRANULARITY_LEVELS = {
    'week': "%Y-w%V",
    'month': "%Y-%m",
    'day': "%Y-%m-%d"
}


def period_formatstring(period):
    return GRANULARITY_LEVELS.get(period)


def get_commit_graph(repo, period='month'):
    g = nk.Graph(weighted=True)
    date_format = period_formatstring(period)
    modularity = {}
    nodes = {}
    fp_commits = get_fp_chain(repo)
    month = fp_commits[0].authored_datetime.strftime(date_format)
    for parent_idx, commit in enumerate(fp_commits[1:]):
        commit_month = commit.authored_datetime.strftime(date_format)
        if commit_month > month:
            community = nk.community.PLM(g)  # PLP is ~5% faster but coarse
            community.run()
            partition = community.getPartition()
            modularity[month] = (partition.numberOfSubsets(), len(nodes), len(g.edges()))
            month = commit_month
        parent = fp_commits[parent_idx]
        files = []  # files changed in this commit
        for diff in commit.diff(parent):
            fname = diff.a_path
            if fname not in nodes:
                nodes[fname] = g.addNode()
            files.append(fname)
        for i, file1 in enumerate(files[:-1]):
            for file2 in files[i+1:]:
                g.increaseWeight(nodes[file1], nodes[file2], 1)
    return g, modularity, nodes


def get_commit_modularity(repo, period='month'):
    """

    Args:
        repo (git.Repo): repository to analyze
            for file co-commit network modularity.
            repo object can be obtained by function `get_repository(repo_slug)`
        period (str): {'month'|'week'|'day'}: granularity of observations

    Returns:
        pd.DataFrame: dataframe with 3 columns:
            index: (str) date formatted according to the period
            'louvain': louvain modularity of file co-commit network
            'files': number of nodes (i.e. files) in co-commit network
            'edges': number of edges in the network

    """
    g, modularity, nodes = get_commit_graph(repo, period)
    return pd.DataFrame(modularity).T.rename(
        columns={0: 'louvain', 1: 'files', 2: 'edges'})


# user types
NOBODY = 0  # no special role
COMMENTER = 1  # somebody who commented on an issue (but didn't report)
REVIEWER = 2  # somebody who left a review comment
USER = 3  # usually, issue reporter
CONTRIBUTOR = 5  # somebody who contributed code
COLLABORATOR = 7  # somebody with direct commit access

ROLE_NAMES = {
    NOBODY: 'nobody',
    COMMENTER: 'commenter',
    REVIEWER: 'reviewer',
    USER: 'user',  # those who at least reported the issue
    CONTRIBUTOR: 'contributor',
    COLLABORATOR: 'collaborator'
}

# event types
ISSUE = 1
REVIEW = 2
PULL_REQUEST = 4
COMMIT = 8

# comments are not included in the events
EVENT_ROLES = {  # assuming user != reporter
    'subscribed': COLLABORATOR,
    'mentioned': COMMENTER,
    'labeled': COLLABORATOR,
    'closed': COLLABORATOR,
    'merged': COLLABORATOR,
    'referenced': COMMENTER,
    'renamed': COLLABORATOR,
    'review_dismissed': COLLABORATOR,
    'assigned': COLLABORATOR,
    'review_requested': COLLABORATOR,
    'comment_deleted': COLLABORATOR,
    'head_ref_deleted': COLLABORATOR,
    'reopened': COLLABORATOR,
    'unlabeled': COLLABORATOR,
    'milestoned': COLLABORATOR,
    'demilestoned': COLLABORATOR,
    'unassigned': COLLABORATOR,
    'marked_as_duplicate': COLLABORATOR,
    'unlocked': COLLABORATOR,
    'locked': COLLABORATOR,
    'head_ref_restored': COLLABORATOR,
    'unsubscribed': NOBODY,
    'base_ref_changed': COLLABORATOR,
    'review_request_removed': COLLABORATOR,
}


def _role_events(repo_slug):
    """Get all events indicating user roles

    Args:
        repo_slug: github repository slug

    Returns:
        pd.Dataframe: full list of events with columns:
            `date`, `user`, `event_type`, `role`.
            `date`: timestamp when this event happened (a datetime object).
                In most cases, you would want to aggregate on those
            `user`: github login of the user whose affiliation
                is indicated by the event
            `role`: role assumed by this event
                (not the prior history, just this event).

    >>> utils._role_events('mui-org/material-ui').head()
                           date  event_type  role     user
    0 2014-08-18 19:11:54+00:00           8     7  hai-cea
    1 2014-08-18 19:43:21+00:00           8     7  hai-cea
    2 2014-08-18 20:20:46+00:00           8     7  hai-cea
    3 2014-08-18 20:27:12+00:00           8     5  hai-cea
    4 2014-08-18 20:51:32+00:00           8     7  hai-cea

    """
    # get reporters and type
    # get_issue_events returns events for both issues and pull requests
    iss = get_issues_and_prs(repo_slug)
    ie = get_issue_events(repo_slug)
    ie['reporter'] = ie.index.map(iss['user'])
    ie['is_pr'] = ie.index.map(iss['is_pr'])
    ie['role'] = ie['event'].map(EVENT_ROLES)
    ie.loc[ie['user'] == ie['reporter'], 'role'] = USER
    # ie = ie[ie['role'] >= USER]
    ie['event_type'] = ISSUE
    # sometimes events were retrieved after issues so we don't have info
    # events are considered issues by default
    ie.loc[ie['is_pr'].fillna(False), 'event_type'] = PULL_REQUEST

    # review comments
    rc = get_pull_review_comments(repo_slug)[['user', 'created_at']]
    rc['event_type'] = REVIEW
    rc['role'] = REVIEWER

    # commits
    css = get_commits(repo_slug).reset_index()[['author', 'sha', 'authored_at']]
    css = css[pd.notnull(css['author'])]
    repo = get_repository(repo_slug)
    fp_commit_shas = {c.hexsha for c in get_fp_chain(repo)}
    css['role'] = CONTRIBUTOR
    css.loc[css['sha'].isin(fp_commit_shas), 'role'] = COLLABORATOR
    css = css[pd.notnull(css['author'])]
    css['event_type'] = COMMIT

    events = pd.concat([
        ie[['created_at', 'user', 'role', 'event_type']].rename(
            columns={'created_at': 'date'}),
        rc[['created_at', 'user', 'role', 'event_type']].rename(
            columns={'created_at': 'date'}),
        css[['author', 'authored_at', 'role', 'event_type']].rename(
            columns={'author': 'user', 'authored_at': 'date'})
    ], sort=True)
    events['date'] = pd.to_datetime(events['date'])

    return events.sort_values('date').reset_index(drop=True)


def count_events(repo_slug, period='month',
                 issues=False, reviews=False, commits=False, pull_requests=False):
    # TODO: deprecate
    assert issues or commits or pull_requests, \
        "At least one event type should be requested"
    events = _role_events(repo_slug)
    if not issues:
        events = events[events['event_type'] != ISSUE]
    if not reviews:
        events = events[events['event_type'] != REVIEW]
    if not commits:
        events = events[events['event_type'] != COMMIT]
    if not pull_requests:
        events = events[events['event_type'] != PULL_REQUEST]
    date_format = period_formatstring(period)

    return events['event_type'].groupby(
        events['date'].dt.strftime(date_format)).count().rename('count')


def fano_factor(repo_slug, period='month', event_types=(), roles=(),
                timestamp=None, length=None):
    """Compute Fano factor
    https://en.wikipedia.org/wiki/Fano_factor
    Computed as variance / mean

    Args:
        repo_slug (str):
        period (str): either 'month' or 'week'
        event_types (Union[Iterable, set]): event types to include
            default: include everything
        roles (Union[Iterable, set]): roles to include
            default: include everything
        timestamp (Optional[datetime.datetime]): timestamp at which to compute
            Fano factor (this is the ENDING timestamp)
        length (Optional[int]): number of periods to take into account,
            all by default (before the timestamp)

    Returns:
        (float): Fano factor

    >>> fano_factor(repo_slug, period='month', roles=(utils.COLLABORATOR,),
    ...             timestamp='2018-12', length=12)
    64.8233646729346
    """
    events = _role_events(repo_slug)
    date_format = period_formatstring(period)

    if event_types:
        events = events[events['event_type'].isin(event_types)]
    if roles:
        events = events[events['role'].isin(roles)]

    counts = events['event_type'].groupby(
        events['date'].dt.strftime(date_format)).count().rename('count')

    if timestamp:
        counts = counts[counts.index < timestamp]
    if length:
        counts = counts.iloc[-length:]
    return counts.var() / counts.mean()


def _repo_contributors(repo_slug, period='month', min_level=CONTRIBUTOR,
                       end=None):
    """
    Args:
        repo_slug (str): self explanatory
        period (str): either 'month' or 'week'
        min_level (int): only consider users with at least this level of access
        end (str): %Y-%m formatted end date of the observations

    Returns:
        pd.DataFrame:
            rows are dates formatted according to period
            columns are contributor GH usernames
            values are 1 if this contributor contributed at most
                <timeout> periods ago, 0 otherwise.
    """
    events = _role_events(repo_slug)
    events = events.loc[events['role'] >= min_level, ['date', 'user']]
    date_format = period_formatstring(period)
    events['date'] = events['date'].dt.strftime(date_format)
    events.drop_duplicates(inplace=True)
    events['values'] = 1
    rc = events.pivot(index='date', columns='user', values='values')

    idx = pd.date_range(rc.index.min(), end or rc.index.max(),
                        freq=period[0].capitalize()).strftime(date_format)
    contributors = rc.reindex(idx).fillna(0).astype(int)
    if np.nan in contributors:
        contributors = contributors.drop(columns=[np.nan])
    return contributors


def contribution_matrix(repo_slug, period='month', min_level=CONTRIBUTOR,
                        timeout=6, end=None):
    """
    Args:
        repo_slug (str): self explanatory
        period (str): either 'month' or 'week'
        min_level (int): only consider users with at least this level of access
        timeout (int): number of periods since the last contribution for which
            contributor remains active
        end (str): %Y-%m formatted end date of the observations

    Returns:
        pd.DataFrame:
            rows are dates formatted according to period
            columns are contributor GH usernames
            values are 1 if this contributor contributed at most
                <timeout> periods ago, 0 otherwise.
    """
    contributors = _repo_contributors(repo_slug, period, min_level, end=end)
    cm = contributors.copy()  # contributors matrix
    for shift in range(1, timeout+1):
        cm += cm.shift(shift, fill_value=0)
    return (cm > 0).astype(int)


@d.memoize
def package_slug(package_name):
    try:
        package = npm.Package(package_name)
    except npm.PackageDoesNotExist:
        return None
    try:
        url = package.info['repository']['url']
    except (TypeError, KeyError):
        return None
    slug = url.rsplit('github.com/', 1)[-1].strip('/')
    while slug.endswith('.git'):
        slug = slug[:-4]
    return slug


def json_package_name(json_fname):
    try:
        with open(json_fname) as fh:
            return json.load(fh).get('name')
    except (IOError, ValueError):
        return None

# def get_package_repo_slug(package_name):


def repo_package_names(repo_slug):
    """find all package.json in """
    repo = get_repository(repo_slug)

    metadata_fname = 'package.json'

    def gen(workdir):
        for root, dirs, files in os.walk(workdir):
            if metadata_fname in files:
                pkgname = json_package_name(os.path.join(root, metadata_fname))
                if pkgname and package_slug(pkgname) == repo_slug:
                    yield pkgname

    return set(gen(repo.working_dir))


@d.fs_cache(expires=ONE_YEAR)
def repos_package_names():
    """ Get packages included in all projects (repositories)

    Returns:
        pd.Series where index is a repo slug
            and values are comma separated package names
    """
    def pkgname(_, repo_slug):
        logging.info(repo_slug)
        try:
            return ",".join(repo_package_names(repo_slug))
        except:
            return None

    ap = all_projects()
    pkgnames = mapreduce.map(pkgname, ap)

    return pd.Series(pkgnames.values, index=ap.values, name='pkgnames')


def _package_scores(package_name):
    try:
        package = npm.Package(package_name)
    except npm.PackageDoesNotExist:
        return {}
    return {
        'quality': package.quality,
        'maintenance_score': package.maintenance_score,
        'popularity': package.popularity
    }


@d.fs_cache(expires=ONE_YEAR)
def package_scores():
    """Get quality, maintenance and popularity scores for packages returned by
    `repos_package_names()`
    """
    package_names = []  # 35k packages total
    for repo_slug, pkgnames in repos_package_names().iteritems():
        logging.info(repo_slug)
        if pkgnames and pd.notnull(pkgnames):
            package_names.extend(pkgnames.split(","))

    def get_scores(i, package_name):
        logging.info("%d: %s", i, package_name)
        return _package_scores(package_name)

    scores = mapreduce.map(get_scores, package_names)

    return pd.DataFrame(scores, index=package_names)


def multiteaming_index(repo_slug, period='month', min_level=CONTRIBUTOR):
    """
    Returns:
        same format of the dataframe as _repo_contributors,
        but values now are number of repositories user committed to
        in a given month
    """
    contributors = _repo_contributors(repo_slug, period, min_level=min_level)
    multitasking = pd.concat(
        [ghtorrent.user_timeline(user) for user in contributors.columns],
        axis=1, sort=False).reindex(contributors.index).fillna(0).astype(int)
    return contributors * multitasking
