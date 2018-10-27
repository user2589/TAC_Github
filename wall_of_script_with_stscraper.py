## script for gathering GitHub data
## using stscraper script written by Marat

# setup folders, stscraper and get test repos
import pandas as pd
import os
rootdir = os.getcwd() + '/data/'
import stscraper as scraper
gh_api = scraper.GitHubAPI()

import pathlib
pathlib.Path('data/commits').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/commits_raw').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/contributers').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/issue_comments').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/issue_comments_raw').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/issue_events').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/issue_events_raw').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/issues').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/issues_raw').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/pull_comments').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/pull_comments_raw').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/pull_commits').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/pull_commits_raw').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/pull_events').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/pull_events_raw').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/pull_review_comments').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/pull_review_comments_raw').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/pulls').mkdir(parents=True, exist_ok=True)
pathlib.Path('data/pulls_raw').mkdir(parents=True, exist_ok=True)
print('Directory created. Starting job')
print('---')



repo_index = pd.read_csv('34k_dataset_1000_3_10.csv', header=0)
# repo_index = repo_index.iloc[0:2]  # for testing with 2 repos
print(str(repo_index.shape[0]) + ' Repos found in Index.')



#### Repo Commits

# repo_commits: 
# not available: [stats: [total, add, dels], files: files_touched_count]
cols = ['repo', 'commit_id', 'author', 'author_time', 
        'committer', 'committer_time', 'comment_count']
for r in repo_index.itertuples():
    
    if gh_api.project_exists(r.repo):
        repo_commits = pd.DataFrame(gh_api.repo_commits(r.repo))
        df = pd.DataFrame(columns=cols)
        file_name = 'data/commits/' + r.package.replace('/','_') + '.csv'
        file_name_raw = 'data/commits_raw/' + r.package.replace('/','_') + '.csv'
        for item in repo_commits.itertuples():
            row = [r.repo,
                   item.sha,
                   item.author['login'] if item.author else item.commit['author']['email'],
                   item.commit['author']['date'],
                   item.committer['login'] if item.committer else item.commit['committer']['email'],
                   item.commit['committer']['date'],
                   item.commit['comment_count']]
            df = df.append(pd.DataFrame([row], columns=cols), ignore_index=True)
        df.to_csv(file_name, index=False)
        repo_commits.to_csv(file_name_raw, index=False)
        
print('Commits processed')



#### Repo Issues

# repo_issues:
# some fields are captured in events (assignee, label, milestone)
cols = ['repo','issue_number','issue_id','state','created_at','updated_at','closed_at',
        'author', 'author_association', 'reaction_count', 'comment_count']
for r in repo_index.itertuples():
    
    if gh_api.project_exists(r.repo):
        repo_issues = pd.DataFrame(gh_api.repo_issues(r.repo))
        df = pd.DataFrame(columns=cols)
        file_name = 'data/issues/' + r.package.replace('/','_') + '.csv'
        file_name_raw = 'data/issues_raw/' + r.package.replace('/','_') + '.csv'
        for item in repo_issues.itertuples():
            row = [r.repo,
                   item.number,
                   item.id,
                   item.state,
                   item.created_at,
                   item.updated_at,
                   item.closed_at,
                   item.user['login'],
                   item.author_association,
                   item.reactions['total_count'],
                   item.comments]
            df = df.append(pd.DataFrame([row], columns=cols), ignore_index=True)
        df.to_csv(file_name, index=False)
        repo_issues.to_csv(file_name_raw, index=False)
    
print('Issues processed')



# issue_comments:
cols = ['repo','issue_number','comment_id','created_at','updated_at',
        'author', 'author_association', 'reaction_count']
cols_raw = ['author_association', 'body', 'created_at', 'html_url', 'id',
            'issue_url', 'node_id', 'reactions', 'updated_at', 'url', 'user']
repo_count = 0
exception_count = 0

for subdir, dirs, files in os.walk(rootdir + 'issues'):
    for file in files:
        filepath = subdir + os.sep + file
        
        if file.endswith('.csv'):
            issues = pd.read_csv(filepath)
            df = pd.DataFrame(columns=cols)
            df_raw = pd.DataFrame(columns=cols_raw)
            file_name = 'data/issue_comments/' + file
            file_name_raw = 'data/issue_comments_raw/' + file
            repo = issues.repo[0]
            
            for i_number in issues.issue_number:
                try:
                    issue_comments = pd.DataFrame(gh_api.issue_comments(repo, i_number))
                except:
                    exception_count += 1
                else:
                    for item in issue_comments.itertuples():
                        row = [repo,
                               i_number,
                               item.id,
                               item.created_at,
                               item.updated_at,
                               item.user['login'],
                               item.author_association,
                               item.reactions['total_count']]
                        df = df.append(pd.DataFrame([row], columns=cols), ignore_index=True)
                
                    df_raw = df_raw.append(issue_comments, ignore_index=True)
                
            df.to_csv(file_name, index=False)
            df_raw.to_csv(file_name_raw, index=False)
            repo_count += 1

print('Issue Comments')



# issue_events: [id, event, time, actor]
# see types at: https://developer.github.com/v3/issues/events/
cols = ['repo','issue_number','event_id','event','created_at', 'actor']
cols_raw = ['actor', 'assignee', 'assigner', 'commit_id', 'commit_url', 
            'created_at', 'dismissed_review','event', 'id', 'label', 
            'lock_reason','milestone', 'rename', 'node_id', 'project_card', 
            'review_requester','url']
repo_count = 0
exception_count = 0

for subdir, dirs, files in os.walk(rootdir + 'issues'):
    for file in files:
        filepath = subdir + os.sep + file
        
        if file.endswith('.csv'):
            issues = pd.read_csv(filepath)
            df = pd.DataFrame(columns=cols)
            df_raw = pd.DataFrame(columns=cols_raw)
            file_name = 'data/issue_events/' + file
            file_name_raw = 'data/issue_events_raw/' + file
            repo = issues.repo[0]
            
            for i_number in issues.issue_number:
                try:
                    issue_events = pd.DataFrame(gh_api.issue_events(repo, i_number))
                except:
                    exception_count += 1
                else:
                    for item in issue_events.itertuples():
                        row = [repo,
                               i_number,
                               item.id,
                               item.event,
                               item.created_at,
                               item.actor['login']]
                        df = df.append(pd.DataFrame([row], columns=cols), ignore_index=True)
                
                    df_raw = df_raw.append(issue_events, ignore_index=True, sort=False)
                
            df.to_csv(file_name, index=False)
            df_raw.to_csv(file_name_raw, index=False)
            repo_count += 1

print('Issue Events processed')



#### Repo Pull Requests

# repo_pulls:
# not available: [merged_by, comments, review_comments, commits, 
#                 additions, deletions, changed_files]
cols = ['repo','pr_number','pr_id','state','created_at','updated_at',
        'closed_at','merged_at', 'author', 'author_association', 'requested_reviewers']
for r in repo_index.itertuples():
    
    if gh_api.project_exists(r.repo):
        repo_pulls = pd.DataFrame(gh_api.repo_pulls(r.repo))
    #     print('Processing ' + r.package + ' with ' + str(repo_issues.shape[0]) + ' issues')
        df = pd.DataFrame(columns=cols)
        file_name = 'data/pulls/' + r.package.replace('/','_') + '.csv'
        file_name_raw = 'data/pulls_raw/' + r.package.replace('/','_') + '.csv'
        for item in repo_pulls.itertuples():
            row = [r.repo,
                   item.number,
                   item.id,
                   item.state,
                   item.created_at,
                   item.updated_at,
                   item.closed_at,
                   item.merged_at,
                   item.user['login'],
                   item.author_association,
                   item.requested_reviewers]
            df = df.append(pd.DataFrame([row], columns=cols), ignore_index=True)
        df.to_csv(file_name, index=False)
        repo_pulls.to_csv(file_name_raw, index=False)
    
print('PRs processed')



# pull_request_commits: 
# (subset duplicates of repo_commits)
cols = ['repo', 'pr_number', 'commit_id', 'author', 'author_time', 
        'committer', 'committer_time', 'comment_count']
cols_raw = ['author', 'comments_url', 'commit', 'committer', 
            'html_url', 'node_id', 'parents', 'sha', 'url']
repo_count = 0
exception_count = 0

for subdir, dirs, files in os.walk(rootdir + 'pulls'):
    for file in files:
        filepath = subdir + os.sep + file
        
        if file.endswith('.csv'):
            pulls = pd.read_csv(filepath)
            df = pd.DataFrame(columns=cols)
            df_raw = pd.DataFrame(columns=cols_raw)
            file_name = 'data/pull_commits/' + file
            file_name_raw = 'data/pull_commits_raw/' + file
            repo = pulls.repo[0]
            
            for pr_number in pulls.pr_number:
                try:
                    pulls_commits = pd.DataFrame(gh_api.pull_request_commits(repo, pr_number))
                except:
                    exception_count += 1
                else:
                    for item in pulls_commits.itertuples():
                        row = [repo,
                               pr_number,
                               item.sha,
                               item.author['login'] if item.author else item.commit['author']['email'],
                               item.commit['author']['date'],
                               item.committer['login'] if item.committer else item.commit['committer']['email'],
                               item.commit['committer']['date'],
                               item.commit['comment_count']]
                        df = df.append(pd.DataFrame([row], columns=cols), ignore_index=True)
                
                    df_raw = df_raw.append(pulls_commits, ignore_index=True)
                
            df.to_csv(file_name, index=False)
            df_raw.to_csv(file_name_raw, index=False)
            repo_count += 1

print('PR Commits processed')



# review_comments (for pulls); dont capture approvals
# [/reviews; capture approvals and comments as events]
cols = ['repo','pr_number','comment_id','created_at','updated_at',
        'author', 'author_association', 'reaction_count']
cols_raw = ['_links', 'author_association', 'body', 'commit_id', 'created_at',
            'diff_hunk', 'html_url', 'id', 'in_reply_to_id', 'node_id',
            'original_commit_id', 'original_position', 'path', 'position',
            'pull_request_review_id', 'pull_request_url', 'reactions', 'updated_at',
            'url', 'user']
repo_count = 0
exception_count = 0

for subdir, dirs, files in os.walk(rootdir + 'pulls'):
    for file in files:
        filepath = subdir + os.sep + file
        
        if file.endswith('.csv'):
            pulls = pd.read_csv(filepath)
            df = pd.DataFrame(columns=cols)
            df_raw = pd.DataFrame(columns=cols_raw)
            file_name = 'data/pull_review_comments/' + file
            file_name_raw = 'data/pull_review_comments_raw/' + file
            repo = pulls.repo[0]
            
            for pr_number in pulls.pr_number:
                try:
                    pr_review_comments = pd.DataFrame(gh_api.review_comments(repo, pr_number))
                except:
                    exception_count += 1
                else:
                    for item in pr_review_comments.itertuples():
                        row = [repo,
                               pr_number,
                               item.id,
                               item.created_at,
                               item.updated_at,
                               item.user['login'],
                               item.author_association,
                               item.reactions['total_count']]
                        df = df.append(pd.DataFrame([row], columns=cols), ignore_index=True)
                
                    df_raw = df_raw.append(pr_review_comments, ignore_index=True)
                
            df.to_csv(file_name, index=False)
            df_raw.to_csv(file_name_raw, index=False)
            repo_count += 1

print('PR Review Comments')



# issue_comments for PRs:
cols = ['repo','pr_number','comment_id','created_at','updated_at',
        'author', 'author_association', 'reaction_count']
cols_raw = ['author_association', 'body', 'created_at', 'html_url', 'id',
            'issue_url', 'node_id', 'reactions', 'updated_at', 'url', 'user']
repo_count = 0
exception_count = 0

for subdir, dirs, files in os.walk(rootdir + 'pulls'):
    for file in files:
        filepath = subdir + os.sep + file
        
        if file.endswith('.csv'):
            pulls = pd.read_csv(filepath)
            df = pd.DataFrame(columns=cols)
            df_raw = pd.DataFrame(columns=cols_raw)
            file_name = 'data/pull_comments/' + file
            file_name_raw = 'data/pull_comments_raw/' + file
            repo = pulls.repo[0]
            
            for pr_number in pulls.pr_number:
                try:
                    pr_comments = pd.DataFrame(gh_api.issue_comments(repo, pr_number))
                except:
                    exception_count += 1
                else:
                    for item in pr_comments.itertuples():
                        row = [repo,
                               pr_number,
                               item.id,
                               item.created_at,
                               item.updated_at,
                               item.user['login'],
                               item.author_association,
                               item.reactions['total_count']]
                        df = df.append(pd.DataFrame([row], columns=cols), ignore_index=True)
                
                    df_raw = df_raw.append(pr_comments, ignore_index=True)
                
            df.to_csv(file_name, index=False)
            df_raw.to_csv(file_name_raw, index=False)
            repo_count += 1

print('PR Comments processed')


# issue_events for PRs: 
cols = ['repo','pr_number','event_id','event','created_at', 'actor']
cols_raw = ['actor', 'assignee', 'assigner', 'commit_id', 'commit_url', 
            'created_at', 'dismissed_review','event', 'id', 'label', 
            'lock_reason','milestone', 'rename', 'node_id', 'project_card', 
            'review_requester','url']
repo_count = 0
exception_count = 0

for subdir, dirs, files in os.walk(rootdir + 'pulls'):
    for file in files:
        filepath = subdir + os.sep + file
        
        if file.endswith('.csv'):
            pulls = pd.read_csv(filepath)
            df = pd.DataFrame(columns=cols)
            df_raw = pd.DataFrame(columns=cols_raw)
            file_name = 'data/pull_events/' + file
            file_name_raw = 'data/pull_events_raw/' + file
            repo = pulls.repo[0]
            
            for pr_number in pulls.pr_number:
                try:
                    pull_events = pd.DataFrame(gh_api.issue_events(repo, pr_number))
                except:
                    exception_count += 1
                else:
                    for item in pull_events.itertuples():
                        row = [repo,
                               pr_number,
                               item.id,
                               item.event,
                               item.created_at,
                               item.actor['login']]
                        df = df.append(pd.DataFrame([row], columns=cols), ignore_index=True)
                
                    df_raw = df_raw.append(pull_events, ignore_index=True)
                
            df.to_csv(file_name, index=False)
            df_raw.to_csv(file_name_raw, index=False)
            repo_count += 1

print('PR Events')



#### Labels, Topics etc

# repo_labels: 
# would like: topics, releases, milestones, projects
cols = ['repo', 'label_count', 'labels']
df = pd.DataFrame(columns=cols)

for r in repo_index.itertuples():
    if gh_api.project_exists(r.repo):
        repo_labels = gh_api.repo_labels(r.repo)
        row = [r.repo, len(repo_labels), repo_labels]           
        df = df.append(pd.DataFrame([row], columns=cols), ignore_index=True)
        
df.to_csv('data/labels.csv', index=False)
print('Labels processed')



# Contributers: 
for r in repo_index.itertuples(): 
    if gh_api.project_exists(r.repo):
        for i in range(10):
            try:
                contributers = pd.DataFrame(gh_api.project_activity(r.repo))
                file_name = 'data/contributers/' + r.package.replace('/','_') + '.csv'
                contributers.to_csv(file_name, index=False)
                break
            except:
                print('   ... failed to get contributers from ' + r.repo + ': Try ' + str(i+1))
        file_name_raw = 'data/commits_raw/' + r.package.replace('/','_') + '.csv'
        
print('Contributions processed')



print('---')
print('Job Completed.')

