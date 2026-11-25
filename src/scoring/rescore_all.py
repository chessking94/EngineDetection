import logging
import os
from pathlib import Path
import subprocess
import time

import pandas as pd
import sqlalchemy as sa

CONFIG_FILE = os.path.join(Path(__file__).parents[1], 'config.json')


def is_job_running(job_name, engine):
    qry_text = f"""
SELECT
CASE WHEN act.stop_execution_date IS NULL THEN 1 ELSE 0 END AS is_running

FROM msdb.dbo.sysjobs AS job
INNER JOIN msdb.dbo.sysjobactivity AS act ON job.job_id = act.job_id
INNER JOIN msdb.dbo.syssessions AS sess ON sess.session_id = act.session_id
INNER JOIN (
    SELECT MAX(agent_start_date) AS max_agent_start_date FROM msdb.dbo.syssessions
) AS sess_max ON sess.agent_start_date = sess_max.max_agent_start_date

WHERE job.name = '{job_name}'
"""
    return pd.read_sql(qry_text, engine).values[0][0]


def run_job(job_name, engine):
    conn = engine.connect().connection
    csr = conn.cursor()
    csr.execute(f"EXEC msdb.dbo.sp_start_job @job_name = '{job_name}'")
    logging.info(f'SQL job "{job_name}" started')
    is_running = 1
    while is_running:
        time.sleep(10)
        is_running = is_job_running(job_name, engine)
    logging.info(f'SQL job "{job_name}" ended')
    conn.close()


def run_script(script_path, script_name, parameters=None):
    cmd_text = fr'"D:\code\src\EngineDetection\.venv\Scripts\python.exe" {script_name}'
    cmd_text = f'{cmd_text} {parameters}' if parameters else cmd_text
    logging.info(f'Begin command "{cmd_text}"')
    result = subprocess.run(cmd_text, cwd=script_path, capture_output=True, text=True, shell=True)
    if result.returncode != 0:
        logging.critical(f'Error executing command: {result.stderr}')
        raise SystemExit
    logging.info(f'End command "{cmd_text}"')


def main():
    logging.basicConfig(
        format='%(asctime)s\t%(funcName)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )

    db = 'PROD'  # or DEV
    if db == 'DEV':
        conn_str = os.getenv('ConnectionStringOdbcDebug')
    else:
        conn_str = os.getenv('ConnectionStringOdbcRelease')

    connection_url = sa.engine.URL.create(
        drivername='mssql+pyodbc',
        query={'odbc_connect': conn_str},
    )
    engine = sa.create_engine(connection_url)

    # step 0 (optional): Python script create_distribution.py for as many source/time control combinations as necessary
    script_path = os.path.dirname(__file__)
    script_name = 'create_distributions.py'
    parameters = f'-mEvaluation -sControl -tClassical -d{db}'
    run_script(script_path, script_name, parameters)

    parameters = f'-mEvaluation -sControl -tCorrespondence -d{db}'
    run_script(script_path, script_name, parameters)

    parameters = f'-mEvaluation -sLichess -tBullet -d{db}'
    run_script(script_path, script_name, parameters)

    parameters = f'-mEvaluation -sLichess -tBlitz -d{db}'
    run_script(script_path, script_name, parameters)

    parameters = f'-mEvaluation -sLichess -tRapid -d{db}'
    run_script(script_path, script_name, parameters)

    parameters = f'-mEvaluation -sLichess -tClassical -d{db}'
    run_script(script_path, script_name, parameters)

    parameters = f'-mEvaluation -sLichess -tCorrespondence -d{db}'
    run_script(script_path, script_name, parameters)

    # step 1: SQL job "Recalculate Move Scores"
    job_name = 'ChessWarehouse - Recalculate Move Scores'
    run_job(job_name, engine)

    # step 2: SQL job "Recalculate Fact Tables"
    job_name = 'ChessWarehouse - Recalculate Fact Tables'
    run_job(job_name, engine)

    # step 3: Python script StatisticsWarehouse.py for as many source/time control combinations as necessary
    script_path = os.path.dirname(__file__)
    script_name = 'StatisticsWarehouse.py'
    parameters = f'-aEvent -d{db}'
    run_script(script_path, script_name, parameters)

    parameters = f'-aGame -d{db}'
    run_script(script_path, script_name, parameters)

    # parameters = f'-aEvaluation -d{db}'
    # run_script(script_path, script_name, parameters)  # TODO: this is taking forever now

    # step 4: SQL job "Recalculate Fact Table Z-Scores"
    job_name = 'ChessWarehouse - Recalculate Z-Scores'
    run_job(job_name, engine)

    # step 5: SQL job "ChessWarehouse - Update [rpt] Schema Tables"
    job_name = 'ChessWarehouse - Update [rpt] Schema Tables'
    run_job(job_name, engine)

    engine.dispose()


if __name__ == '__main__':
    main()
