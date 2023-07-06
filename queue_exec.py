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

WORKERS_COUNT = 4  # Количество исполнителей
WORKS_COUNT = 40   # Количество задач

# set up logging to console
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

logging.basicConfig(level=logging.DEBUG,
                    handlers=[stream_handler])


class Worker:
    """Класс Worker представляет собой исполнителя задачи"""

    def __init__(self, worker_id: int):
        self.worker_id = worker_id

    @staticmethod
    def __next_task(tnx: TransactionManager):
        """Извлекает следующую задачу из очереди.

        Arguments:
            tnx: Транзакция в которой выполняется запрос
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
        """Срабатывает при старте выполнения задачи.

        Устанавливает задаче признак того, что она запущена и время старта.

        Arguments:
            tnx: Транзакция в которой выполняется запрос
            task_id: Идентификатор задачи
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
        """Срабатывает при завершении выполнения задачи.

        Устанавливает задаче время завершения и статус с которым завершилась задача.

        Arguments:
            tnx: Транзакция в которой выполняется запрос
            task_id: Идентификатор задачи
            status: Код статуса завершения. 0 - успешно, 1 - завершено с ошибкой
            status_text: Текст статуса завершения. При успешном завершении записываем "OK",
                в противном случае текст ошибки.
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
        """Этот метод приведён как пример функции выполнения некоторой задачи.

        В реальных задачах он будет другим и с другим набором параметров.

        Arguments:
            task_id: Идентификатор задачи
            name: Имя задачи
        """
        # выбор случайной задержки
        t = random.randint(1, 4)
        time.sleep(t * 0.01)
        # для демонстрации того, что задача может выполняться с ошибками,
        # генерируем исключение для двух из случайных чисел.
        if t == 3:
            raise Exception("Some error")

    def run(self) -> int:
        """Выполнение задачи"""
        conflict_counter = 0
        # Для параллельного выполнения каждый поток должен иметь своё соединение с БД.
        with connect(DB_URI, user=DB_USER, password=DB_PASSWORD, charset=DB_CHARSET) as con:
            tnx = con.transaction_manager(tpb(Isolation.SNAPSHOT, lock_timeout=0, access_mode=TraAccessMode.WRITE))
            while True:
                # Извлекаем очередную задачу и ставим ей признак того что она выполняется.
                # Поскольку задача может выполниться с ошибкой, то признак старта задачи
                # выставляем в отдельной транзакции.
                tnx.begin()
                try:
                    task_row = self.__next_task(tnx)
                    # Если задачи закончились завершаем поток
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

                # Выполняем задачу
                status = 0
                status_text = "OK"
                try:
                    self.on_task_execute(task_id, name)
                except Exception as err:
                    # Если при выполнении возникла ошибка,
                    # то ставим соответствующий код статуса и сохраняем текст ошибки.
                    status = 1
                    status_text = f"{err}"
                    # logging.error(status_text)

                # Сохраняем время завершения задачи и записываем статус её завершения.
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
        # Чистим предыдущие задачи
        con.begin()
        with con.cursor() as cur:
            cur.execute("DELETE FROM QUEUE_TASK")
        con.commit()
        # Постановщик ставит 40 задач
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

    # создаём исполнителей
    workers = map(lambda worker_id: Worker(worker_id), range(WORKERS_COUNT))
    with pool.ProcessPoolExecutor(max_workers=WORKERS_COUNT) as executer:
        features = map(lambda worker: executer.submit(worker.run), workers)
        conflicts = map(lambda feature: feature.result(), pool.as_completed(features))
        conflict_count = sum(conflicts)

    # считаем статистику
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

        columns = ["ID", "NAME", "STARTED", "WORKER_ID", "START_TIME", "FINISH_TIME",
                   "FINISH_STATUS", "STATUS_TEXT"]

        table = PrettyTable(columns)
        table.add_rows(rows)
        print("\nTasks:")
        print(table)


if __name__ == "__main__":
    main()
