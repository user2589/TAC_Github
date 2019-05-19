
import pandas as pd
from sqlalchemy import create_engine

USER = 'ghtorrent_user'
PASSWORD = 'ghtorrent_password'
HOST = 'localhost'
DATABASE = 'ghtorrent-2018-03'

engine = create_engine(
    'mysql+mysqldb://{user}:{password}@{host}/{database}?charset=utf8mb4'
    ''.format(user=USER, password=PASSWORD, host=HOST, database=DATABASE),
    pool_recycle=3600)


# same as utils.GRANULARITY_LEVELS,
# but MySQL is using lowercase "v" for 1-based weeks starting on Monday
# instead of capital "V"
DATE_FORMATS = {
    'week': "%Y-w%v",
    'month': "%Y-%m",
    'day': "%Y-%m-%d"
}


def user_timeline(user, period='month'):
    return pd.read_sql("""
        SELECT DATE_FORMAT(c.created_at, %(date_format)s) as month, count(distinct c.project_id) as cnt 
        FROM commits c, users u 
        WHERE c.author_id = u.id AND u.login=%(user)s 
        GROUP BY DATE_FORMAT(c.created_at, %(date_format)s) order by month
        """,
        engine,
        params={'user':user, 'date_format': DATE_FORMATS[period]}
    ).set_index('month')['cnt'].rename(user)

