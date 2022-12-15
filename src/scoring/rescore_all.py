import json
import logging
import os
import time

import pandas as pd
import pyodbc as sql


def get_conf(key):
    fname = r'C:\Users\eehunt\Repository\confidential.json'
    with open(fname, 'r') as t:
        key_data = json.load(t)
    val = key_data.get(key)
    return val


def is_job_running(job_name, conn):
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
    return pd.read_sql(qry_text, conn).values[0][0]


def run_job(job_name, conn, csr):
    csr.execute(f"EXEC msdb.dbo.sp_start_job @job_name = '{job_name}'")
    logging.info(f'SQL job "{job_name}" started')
    is_running = 1
    while is_running:
        time.sleep(10)
        is_running = is_job_running(job_name, conn)
    logging.info(f'SQL job "{job_name}" ended')


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

    conn_str = get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)
    csr = conn.cursor()

    # step 0 (optional): Python script create_distribution.py for as many source/time control combinations as necessary
    # script_path = r'C:\Users\eehunt\Repository\EngineDetection\src\scoring'
    # script_name = 'create_distributions.py'
    # parameters = '-sLichess -tBlitz'
    # run_script(script_path, script_name, parameters)

    # step 1: SQL job "Recalculate Move Scores"
    job_name = 'Recalculate Move Scores'
    run_job(job_name, conn, csr)

    # step 2: SQL job "Recalculate Fact Tables"
    job_name = 'Recalculate Fact Tables'
    run_job(job_name, conn, csr)

    # step 3: Python script StatisticsWarehouse.py for as many source/time control combinations as necessary
    script_path = r'C:\Users\eehunt\Repository\EngineDetection\src\scoring'
    script_name = 'StatisticsWarehouse.py'
    parameters = '-aEvent'
    run_script(script_path, script_name, parameters)

    parameters = '-aGame'
    run_script(script_path, script_name, parameters)

    """Not currently using the Evaluation aggregation, going to disable from recalc until it's actually used"""
    # parameters = '-aEvaluation'
    # run_script(script_path, script_name, parameters)

    # step 4: SQL job "Recalculate Fact Table Z-Scores"
    job_name = 'Recalculate Fact Table Z-Scores'
    run_job(job_name, conn, csr)

    # step 5: SQL job "Index Maintenance.Subplan_1"
    job_name = 'Index Maintenance.Subplan_1'
    run_job(job_name, conn, csr)

    conn.close()


if __name__ == '__main__':
    main()
