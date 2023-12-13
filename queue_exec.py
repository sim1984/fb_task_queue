#!/usr/bin/python3

import concurrent.futures as pool
import logging
import random
import time

from firebird.driver import connect, DatabaseError
from firebird.driver import driver_config
from firebird.driver import tpb, Isolation, TraAccessMode
from firebird.driver.core import TransactionManager
from prettytable import PrettyTable

driver_config.fb_client_library.value = "c:\\firebird\\5.0\\fbclient.dll"

DB_URI = 'inet://localhost:3055/d:\\fbdata\\5.0\\queue.fdb'
DB_USER = 'SYSDBA'
DB_PASSWORD = 'masterkey'
DB_CHARSET = 'UTF8'

WORKERS_COUNT = 4  # Number of Executors
WORKS_COUNT = 40   # Number of Tasks

# set up logging to console
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

logging.basicConfig(level=logging.DEBUG,
                    handlers=[stream_handler])


class Worker:
    """Class Worker is am executor"""

    def __init__(self, worker_id: int):
        self.worker_id = worker_id

    @staticmethod
    def __next_task(tnx: TransactionManager):
        """Retrieves the next task from the queue.

        Arguments:
            tnx: The transaction in which the request is executed
        """

        cur = tnx.cursor()

        cur.execute("""
            SELECT ID, NAME
            FROM QUEUE_TASK
            WHERE STARTED IS FALSE
            ORDER BY STARTED, ID
            FETCH FIRST ROW ONLY 
            FOR UPDATE WITH LOCK SKIP LOCKED
        """)

        row = cur.fetchone()
        cur.close()
        return row

    def __on_start_task(self, tnx: TransactionManager, task_id: int) -> None:
        """Fires when task execution starts.

        Sets the flag to the task to indicate that it is running, and sets the start time of the task.

        Arguments:
            tnx: The transaction in which the request is executed
            task_id: Task ID
        """

        cur = tnx.cursor()
        cur.execute(
            """
            UPDATE QUEUE_TASK 
            SET 
                STARTED = TRUE, 
                WORKER_ID = ?,
                START_TIME = CURRENT_TIMESTAMP 
            WHERE ID = ?
            """,
            (self.worker_id, task_id,)
        )

    @staticmethod
    def __on_finish_task(tnx: TransactionManager, task_id: int, status: int, status_text: str) -> None:
        """Fires when a task completes.

        Sets the task completion time and the status with which the task completed.

        Arguments:
            tnx: The transaction in which the query is executed
            task_id: Task ID
            status: Completion status code. 0 - successful, 1 - completed with error
            status_text: Completion status text. If successful, write "OK",
                otherwise the error text.
        """

        cur = tnx.cursor()
        cur.execute(
            """
                UPDATE QUEUE_TASK 
                SET 
                    FINISH_STATUS = ?, 
                    STATUS_TEXT = ?,
                    FINISH_TIME = CURRENT_TIMESTAMP 
                WHERE ID = ?
            """,
            (status, status_text, task_id,)
        )

    def on_task_execute(self, task_id: int, name: str) -> None:
        """This method is given as an example of a function to perform some task.

        In real problems it could be different and with a different set of parameters.

        Arguments:
            task_id: Task ID
            name: Task Name
        """

        # let get random delay
        t = random.randint(1, 4)
        time.sleep(t * 0.01)
        # to demonstrate that a task can be performed with errors,
        # let's generate an exception for two of the random numbers.
        if t == 3:
            raise Exception("Some error")

    def run(self) -> int:
        """Task Execution"""
        conflict_counter = 0
        # For parallel execution, each thread must have its own connection to the database.
        with connect(DB_URI, user=DB_USER, password=DB_PASSWORD, charset=DB_CHARSET) as con:
            tnx = con.transaction_manager(tpb(Isolation.SNAPSHOT, lock_timeout=0, access_mode=TraAccessMode.WRITE))
            while True:
                # We extract the next outstanding task and give it a sign that it is being executed.
                # Since the task may be executed with an error, the task start sign
                # is set in the separate transaction.
                tnx.begin()
                try:
                    task_row = self.__next_task(tnx)
                    # If the tasks are finished, we terminate the thread
                    if task_row is None:
                        tnx.commit()
                        break
                    (task_id, name,) = task_row
                    self.__on_start_task(tnx, task_id)
                    tnx.commit()
                except DatabaseError as err:
                    if err.sqlstate == "40001":
                        conflict_counter = conflict_counter + 1
                        logging.error(f"Worker: {self.worker_id}, Task: {self.worker_id}, Error: {err}")
                    else:
                        logging.exception('')
                    tnx.rollback()
                    continue

                # Execute task
                status = 0
                status_text = "OK"
                try:
                    self.on_task_execute(task_id, name)
                except Exception as err:
                    # If an error occurs during execution,
                    # then set the appropriate status code and save the error text.
                    status = 1
                    status_text = f"{err}"
                    # logging.error(status_text)

                # We save the task completion time and record its completion status.
                tnx.begin()
                try:
                    self.__on_finish_task(tnx, task_id, status, status_text)
                    tnx.commit()
                except DatabaseError:
                    if err.sqlstate == "40001":
                        conflict_counter = conflict_counter + 1
                        logging.error(f"Worker: {self.worker_id}, Task: {self.worker_id}, Error: {err}")
                    else:
                        logging.exception('')
                    tnx.rollback()

        return conflict_counter


def main():
    print(f"Start execute script. Works: {WORKS_COUNT}, workers: {WORKERS_COUNT}\n")

    with connect(DB_URI, user=DB_USER, password=DB_PASSWORD, charset=DB_CHARSET) as con:
        # Clean previous tasks from the queue
        con.begin()
        with con.cursor() as cur:
            cur.execute("DELETE FROM QUEUE_TASK")
        con.commit()
        # Task Manager sets 40 tasks
        con.begin()
        with con.cursor() as cur:
            cur.execute(
                """
                EXECUTE BLOCK (CNT INTEGER = ?)
                AS
                DECLARE I INTEGER;
                BEGIN
                  I = 0;
                  WHILE (I < CNT) DO
                  BEGIN
                    I = I + 1;
                    INSERT INTO QUEUE_TASK(NAME)
                    VALUES ('Task ' || :I);
                  END
                END
                """,
                (WORKS_COUNT,)
            )
        con.commit()

    # Let's create executors
    workers = map(lambda worker_id: Worker(worker_id), range(WORKERS_COUNT))
    with pool.ProcessPoolExecutor(max_workers=WORKERS_COUNT) as executer:
        features = map(lambda worker: executer.submit(worker.run), workers)
        conflicts = map(lambda feature: feature.result(), pool.as_completed(features))
        conflict_count = sum(conflicts)

    # read statistics
    with connect(DB_URI, user=DB_USER, password=DB_PASSWORD, charset=DB_CHARSET) as con:
        cur = con.cursor()
        cur.execute("""
            SELECT
              COUNT(*) AS CNT_TASK,
              COUNT(*) FILTER(WHERE STARTED IS TRUE AND FINISH_TIME IS NULL) AS CNT_ACTIVE_TASK,
              COUNT(*) FILTER(WHERE FINISH_TIME IS NOT NULL) AS CNT_FINISHED_TASK,
              COUNT(*) FILTER(WHERE FINISH_STATUS = 0) AS CNT_SUCCESS,
              COUNT(*) FILTER(WHERE FINISH_STATUS = 1) AS CNT_ERROR,
              AVG(DATEDIFF(MILLISECOND FROM START_TIME TO FINISH_TIME)) AS AVG_ELAPSED_TIME,
              DATEDIFF(MILLISECOND FROM MIN(START_TIME) TO MAX(FINISH_TIME)) AS SUM_ELAPSED_TIME,
              CAST(? AS BIGINT) AS CONFLICTS
            FROM QUEUE_TASK        
        """, (conflict_count,))
        row = cur.fetchone()
        cur.close()

        stat_columns = ["TASKS", "ACTIVE_TASKS", "FINISHED_TASKS", "SUCCESS", "ERROR", "AVG_ELAPSED_TIME",
                        "SUM_ELAPSED_TIME", "CONFLICTS"]

        stat_table = PrettyTable(stat_columns)
        stat_table.add_row(row)
        print("\nStatistics:")
        print(stat_table)

        cur = con.cursor()
        cur.execute("""
            SELECT
              ID,
              NAME,
              STARTED,
              WORKER_ID,
              START_TIME,
              FINISH_TIME,
              FINISH_STATUS,
              STATUS_TEXT
            FROM QUEUE_TASK       
        """)
        rows = cur.fetchall()
        cur.close()

        columns = ["ID", "NAME", "STARTED", "WORKER", "START_TIME", "FINISH_TIME",
                   "STATUS", "STATUS_TEXT"]

        table = PrettyTable(columns)
        table.add_rows(rows)
        print("\nTasks:")
        print(table)


if __name__ == "__main__":
    main()
