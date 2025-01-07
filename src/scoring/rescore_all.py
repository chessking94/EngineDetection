import logging
import os
from pathlib import Path
import time

import pandas as pd
import sqlalchemy as sa

CONFIG_FILE = os.path.join(Path(__file__).parents[1], 'config.json')


def is_job_running(job_name, engine):
    qry_text = f"""
SELECT
CASE WHEN act.stop_execution_date IS NULL THEN 1 ELSE 0 END AS is_running

FROM msdb.dbo.sysjobs job
JOIN msdb.dbo.sysjobactivity act ON
    job.job_id = act.job_id
JOIN msdb.dbo.syssessions sess ON
    sess.session_id = act.session_id
JOIN (
    SELECT
    MAX(agent_start_date) AS max_agent_start_date
    FROM msdb.dbo.syssessions
) sess_max ON
    sess.agent_start_date = sess_max.max_agent_start_date

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
    cmd_text = f'py {script_name}'
    cmd_text = f'{cmd_text} {parameters}' if parameters else cmd_text
    logging.info(f'Begin command "{cmd_text}"')
    if os.getcwd != script_path:
        os.chdir(script_path)
    os.system('cmd /C ' + cmd_text)
    logging.info(f'End command "{cmd_text}"')


def main():
    logging.basicConfig(
        format='%(asctime)s\t%(funcName)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )

    conn_str = os.getenv('ConnectionStringOdbcRelease')
    connection_url = sa.engine.URL.create(
        drivername='mssql+pyodbc',
        query={"odbc_connect": conn_str}
    )
    engine = sa.create_engine(connection_url)

    # step 0 (optional): Python script create_distribution.py for as many source/time control combinations as necessary
    script_path = os.path.dirname(__file__)
    script_name = 'create_distributions.py'
    parameters = '-mEvaluation -sControl -tClassical'
    run_script(script_path, script_name, parameters)

    parameters = '-mEvaluation -sControl -tCorrespondence'
    run_script(script_path, script_name, parameters)

    parameters = '-mEvaluation -sLichess -tBullet'
    run_script(script_path, script_name, parameters)

    parameters = '-mEvaluation -sLichess -tBlitz'
    run_script(script_path, script_name, parameters)

    parameters = '-mEvaluation -sLichess -tRapid'
    run_script(script_path, script_name, parameters)

    parameters = '-mEvaluation -sLichess -tClassical'
    run_script(script_path, script_name, parameters)

    parameters = '-mEvaluation -sLichess -tCorrespondence'
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
    parameters = '-aEvent'
    run_script(script_path, script_name, parameters)

    parameters = '-aGame'
    run_script(script_path, script_name, parameters)

    parameters = '-aEvaluation'
    run_script(script_path, script_name, parameters)

    # step 4: SQL job "Recalculate Fact Table Z-Scores"
    job_name = 'ChessWarehouse - Recalculate Z-Scores'
    run_job(job_name, engine)

    engine.dispose()


if __name__ == '__main__':
    main()
