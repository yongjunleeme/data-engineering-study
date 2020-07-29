"""
SQL이나  Python으로 Monthly Active User 세보기
- 앞서 주어진 두개의 테이블 (raw_data.session_timestamp, raw_data.user_session_channel)을 바탕으로 월별마다 액티브한 사용자들의 수를 카운트한다.
- 여기서 중요한 점은 세션의 수를 세는 것이 아니라 사용자의 수를 카운트한다는 점이다.
- 결과는 예를 들면 아래와 같은 식이 되어야 한다:
   - 2019-05: 400
   - 2019-06: 500
   - 2019-07: 600
   - ...
"""
import psycopg2
import account

def get_Redshift_connection():
    host = "grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = account.redshift_user
    redshift_pass = account.redshift_pass
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(readonly=True, autocommit=True)
    return conn.cursor()

cur = get_Redshift_connection()

sql = "SELECT to_char(st.ts, 'YYYY-MM') as Monthly, count(DISTINCT usc.userid) as ActiveUser FROM raw_data.session_timestamp st JOIN raw_data.user_session_channel usc ON st.sessionid = usc.sessionid GROUP BY to_char(st.ts, 'YYYY-MM') ORDER BY 1"

cur.execute(sql)
rows = cur.fetchall()

for r in rows:
  print(r)

# import pandas.io.sql as sqlio
# df = sqlio.read_sql_query(sql,conn)
# df.head()
