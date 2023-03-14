import time
import logging
import urllib

import sqlalchemy

def consume(engine):
    sql = sqlalchemy.text("""UPDATE queue SET is_done=True WHERE itemid= (
                             SELECT itemid 
                             FROM queue
                             WHERE NOT is_done
                             ORDER BY itemid
                             FOR UPDATE SKIP LOCKED
                             LIMIT 1)
                             RETURNING *
                          """)

    con = engine.connect()
    trans = con.begin()
    try:
        job_id = con.execute(sql).all()[0][0]
    except:
        job_id = None

    if job_id:
        logging.info(f"Starting job {job_id}...")
        execute_job()
    
        trans.commit()
        logging.info(f"Job {job_id} finished.")
    else:
        logging.info(f"No job queued.")
        time.sleep(10)


def get_engine():
    connectionstring = urllib.parse.urlparse(f"postgresql://markuskurbel@localhost:5432/postgres")
    url_parsed_connection_string = f"{connectionstring.scheme}://{connectionstring.username}:" \
                                   f"@{connectionstring.hostname}" \
                                   f"{connectionstring.path}"
    return sqlalchemy.create_engine(url_parsed_connection_string)


def execute_job():
    time.sleep(20)


if __name__ == '__main__':
    logging.basicConfig(format='[%(asctime)s][%(levelname)s]: %(message)s',
                    level=logging.INFO)
    engine = get_engine()

    while True:
        consume(engine)
