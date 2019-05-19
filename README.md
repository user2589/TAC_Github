### Transactive Attentional Control (TAC) in Open Source Software Teams

Python code to mine github repository data for NPM packages.
Goal is to study Transactive Attentional Control (TAC) behaviors within open source teams.

Pranav Gupta & Marat Valiev
Carnegie Mellon University



Available data/functions:
------------------------

All reusable functions are placed in `utils.py`.

First of all, please check user roles and comments in their defitions
(search for `NOBODY =`).
There is a group of similar definitions for event types.

`_role_events(repo_slug)`:
    returns a dataframe with full list of events 
    (see docstring for column definitions).
    We do not use this function directly, 
    but it allows to get a lot of information we need, for example:
    
    ```python
    events = utils._role_events('mui-org/material-ui')
    # now, get list of users and roles when they first took it
    first_roles = events[['date', 'role', 'user']].groupby(['user', 'role']).first()
    first_roles.head(12)
    
        user         role                          
    0x0ece       5    2015-02-24 00:49:14+00:00
    170102       2    2017-04-19 17:29:18+00:00
    17x          3    2017-12-27 07:52:49+00:00
    1attice      3    2018-06-08 20:03:40+00:00
    1nd          1    2018-02-22 09:42:50+00:00
    2juicy       3    2018-08-31 05:05:34+00:00
    3rwww1       1    2018-10-15 08:05:14+00:00
                 7    2018-10-15 08:05:14+00:00
    4xDMG        3    2018-01-17 21:04:06+00:00
    59naga       2    2016-03-11 19:01:03+00:00
                 5    2016-05-15 22:32:40+00:00
    ```
    
`count_events(repo_slug, period='month',
              issues=False, reviews=False, commits=False, pull_requests=False)`
    returns count of events of specific type.
    It is not very useful by itself but you can use its code as an example of
    how to get event counts. For example, here we get issue events:

    ```python    
    issue_events_count = utils.count_events(repo_slug, period='week', issues=True)

    date
    2017-w45    354
    2017-w46    550
    2017-w47    296
    2017-w48    364
    2017-w49    495
    ```
    
`fano_factor(repo_slug, period='month', event_types=(), roles=(),
             timestamp=None, length=None)`
    please see the docstring.
    
    
`_repo_contributors(repo_slug, period='month', min_level=CONTRIBUTOR)`
    Get a dataframe with timestamps in index and users in columns; 
    values are 1 when user contributed somethign in the specified role 
    and 0 otherwise.

`contribution_matrix(repo_slug, period='month', min_level=CONTRIBUTOR,
                     timeout=6)`
    Similar to `_repo_contributors`, but "stretches" participation for the next 
    `timeout` periods.
                     
       
`repos_package_names()`
    Get package names hosted in all repositories.
    Note that some repositories do not represent any packages, 
    e.g. forks of original repos.


`package_scores()`
    Get a dataframe with popularity, maintenance and quality scores 
    of packages identified by `repos_package_names()`.
    Index is package names, three columns are 
    `maintenance_score`, `popularity`, and `quality`.
    
    
`multiteaming_index(repo_slug, period='month', min_level=CONTRIBUTOR)`
    See the docstring.
    Typically, you would want to sum up on rows, perhaps weighting by number of commits.