#!/usr/bin/env python

import argparse
import logging

from utils import *

EVENTS_COMMIT = 0
EVENTS_COMMIT_FP = 1
EVENTS_ISSUE = 2
EVENTS_PULL_REQUEST = 3
EVENTS_ISSUE_COMMENT = 4
EVENTS_REVIEW_COMMENT = 5

metrics = {
    'commits': get_commits,
    'issues': get_issues,
    'issue_comments': get_issue_comments,
    # 'issue_events': get_issue_events,
    'pulls': get_pulls,
    'pull_review_comments': get_pull_review_comments,
}


fs_cache = d.fs_cache('features')


@fs_cache
def events_log(repo):
    logging.info('Processing %s', repo)
    # TODO: repo events log
    #     Col: unique_id
    #     Col: event_type (commit, issue, issue_comment, pull_request,
    #           pull_review_comment OR one of the events in issue_events)
    #     Col: parent_id (empty for commit, issue and PR,
    #           issue_id for comments and events)
    #     Col: created_at
    #     Col: user

    data = {metric: provider(repo) for metric, provider in metrics.items()}

    # TODO: when to count commits? {authored, committed, or merged to master}
    # TODO: implement COMMIT_FP
    data['commits'].reset_index(inplace=True)
    data['commits']['sha'] = data['commits']['sha'].str[:8]
    data['commits']['event'] = EVENTS_COMMIT
    data['commits']['parent'] = None
    data['commits'].rename(
        columns={'author': 'user',
                 'authored_at': 'created_at',
                 'sha': 'id'},
        inplace=True)

    data['issues'].reset_index(inplace=True)
    data['issues']['event'] = EVENTS_ISSUE
    data['issues']['id'] = data['issues']['number']
    data['issues']['parent'] = None

    data['pulls'].reset_index(inplace=True)
    data['pulls']['event'] = EVENTS_PULL_REQUEST
    data['pulls']['id'] = data['pulls']['number']
    data['pulls']['parent'] = None

    data['issue_comments'].reset_index(inplace=True)
    data['issue_comments']['event'] = EVENTS_ISSUE_COMMENT
    data['issue_comments'].rename(
        columns={'issue_no': 'parent'}, inplace=True)

    data['pull_review_comments'].reset_index(inplace=True)
    data['pull_review_comments']['event'] = EVENTS_REVIEW_COMMENT
    data['pull_review_comments'].rename(
        columns={'pr_no': 'parent'}, inplace=True)

    # TODO: not sure if we need issue events
    # note - merging pull request generates burst of three events:
    #   merged, closed, referenced (from the merge commit message)

    columns = ('event', 'id', 'user', 'created_at', 'parent')

    def gen():
        for metric, df in data.items():
            logging.warning(metric)
            yield df.loc[:, columns]

    return pd.concat(gen())


def workload(repo):
    # TODO: workload: number of issues, commits, PRs
    pass


def user_roles_log(repo):
    # TODO: user roles, per project - current status
    # roles: owner, member, contributor, reporter, commenter
    # to be obtained from events log
    pass


def collect_data(repo, row):
    events_log(repo)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Build feature files for TAC project")
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
    #     events_log(repository, row)
