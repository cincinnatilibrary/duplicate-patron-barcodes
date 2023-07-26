import psycopg2
from contextlib import contextmanager
from functools import wraps

def retry(max_attempts=5, initial_delay=5, backoff_factor=5):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"Attempt {attempt+1} failed with error: {e}")
                    if attempt < max_attempts - 1:  # i.e., if it's not the last attempt
                        print(f"Waiting {delay} seconds before retrying...")
                        time.sleep(delay)  # pause execution for 'delay' seconds
                        delay *= backoff_factor  # increase the delay
                        continue  # go to the next attempt
                    else:  # this was the last attempt
                        raise  # re-raise the last exception
        return wrapper
    return decorator


# returns first columns, then the rows
@retry(max_attempts=5)
def execute_query_yield_rows(cursor, query, params):
    cursor.execute(query, params)
    columns = [description[0] for description in cursor.description]
    yield columns
    row = cursor.fetchone()
    while row is not None:
        yield row
        row = cursor.fetchone()


@contextmanager
def get_cursor(dsn):
    try:
        con = psycopg2.connect(dsn=dsn)
        cursor = con.cursor()
        yield cursor

    finally:
        cursor.close()
        del(cursor)
        con.close()
        del(con)
            

# test out our connection / example use
# with get_cursor(dsn=dsn) as cursor:
#     sql = """\
#     SELECT
#         *
#     FROM
#         sierra_view.circ_trans
#     ORDER BY
#         id DESC
#     LIMIT 100
#     """
#     rows = execute_query_yield_rows(cursor, sql, None)
#     # print(result)
    
#     with open('test.csv', 'w') as f:
#         writer = csv.writer(f)
#         columns = next(rows)
#         writer.writerow(columns)
#         for i, row in enumerate(rows):
#             writer.writerow(row)
#             if i % 100000 == 0:
#                 print('.', end='')
#         print(f'done ({i+1})')