import argparse
import json
import logging
from statistics import NormalDist

import numpy as np
import pandas as pd
import pyodbc as sql
from scipy import stats, optimize

SRC_CHOICES = ['Personal', 'PersonalOnline', 'Control', 'Lichess', 'Test']
TC_CHOICES = ['Ultrabullet', 'Bullet', 'Blitz', 'Rapid', 'Classical', 'Correspondence']


def convert_names(conn, src, tc):
    qry_text = f"""
SELECT
SourceID,
TimeControlID
FROM dim.Sources s
CROSS APPLY dim.TimeControls t
WHERE s.SourceName = '{src}'
AND t.TimeControlName = '{tc}'
"""
    logging.debug(f'Select query|{qry_text}')
    return pd.read_sql(qry_text, conn).values[0].tolist()


def get_conf(key):
    fname = r'C:\Users\eehunt\Repository\confidential.json'
    with open(fname, 'r') as t:
        key_data = json.load(t)
    val = key_data.get(key)
    return val


def get_curr_parameters(conn, srcid, tcid):
    qry_text = f"""
SELECT
Mean,
StandardDeviation

FROM stat.EvalDistributionParameters

WHERE SourceID = {srcid}
AND TimeControlID = {tcid}
"""
    return pd.read_sql(qry_text, conn).to_numpy()[0]


def get_parameters(conn, srcid, tcid, recalc=False):
    if recalc:
        actual = observed_values(conn, srcid, tcid)
        params = optimize.curve_fit(f=stats.norm.cdf, xdata=actual['Evaluation'], ydata=actual['Win_Percentage'], p0=[0, 1])[0]

        csr = conn.cursor()
        sql_del = f'DELETE FROM stat.EvalDistributionParameters WHERE SourceID = {srcid} AND TimeControlID = {tcid}'
        logging.debug(f'Delete query|{sql_del}')
        csr.execute(sql_del)
        conn.commit()

        sql_ins = 'INSERT INTO stat.EvalDistributionParameters (SourceID, TimeControlID, Mean, StandardDeviation) '
        sql_ins = sql_ins + f"VALUES ({srcid}, {tcid}, {params[0]}, {params[1]})"
        logging.debug(f'Insert query|{sql_ins}')
        csr.execute(sql_ins)
        conn.commit()
    else:
        params = get_curr_parameters(conn, srcid, tcid)
    dict = {
        'Mean': params[0],
        'StandardDeviation': params[1]
    }
    return dict


def observed_values(conn, srcid, tcid):
    qry_text = f"""
SELECT
ROUND(CAST(m.T1_Eval AS float), 1) AS Evaluation,
AVG(g.Result) AS Win_Percentage

FROM lake.Moves m
JOIN lake.Games g ON
    m.GameID = g.GameID
JOIN dim.Sources s ON
    g.SourceID = s.SourceID
JOIN dim.TimeControlDetail td ON
    g.TimeControlDetailID = td.TimeControlDetailID

WHERE g.SourceID = {srcid}
AND td.TimeControlID = {tcid}
AND (CASE WHEN m.T1_Eval LIKE '#%' THEN 100 ELSE ROUND(CAST(m.T1_Eval AS float), 1) END) BETWEEN -3 AND 3

GROUP BY
ROUND(CAST(m.T1_Eval AS float), 1)
"""
    return pd.read_sql(qry_text, conn)


def validate_args(src, tc):
    val_src = src
    val_tc = tc
    if tc == 'Ultrabullet':
        logging.critical(f'Process not developed for {tc}, process terminated')
        raise SystemExit

    if src not in ['Control', 'Lichess']:
        val_src = 'Control'

    if tc in ['Bullet', 'Blitz', 'Rapid']:
        val_src = 'Lichess'

    return [val_src, val_tc]


def main():
    logging.basicConfig(
        format='%(funcName)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )

    vrs_num = '2.0'
    parser = argparse.ArgumentParser(
        description='Evalation Distribution Calculator',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        usage=argparse.SUPPRESS
    )
    parser.add_argument(
        '-v', '--version',
        action='version',
        version='%(prog)s ' + vrs_num
    )
    parser.add_argument(
        '-s', '--source',
        default='Lichess',
        choices=SRC_CHOICES,
        help='Data source'
    )
    parser.add_argument(
        '-t', '--timecontrol',
        default='Rapid',
        choices=TC_CHOICES,
        help='Time control'
    )

    args = parser.parse_args()
    config = vars(args)
    src = config['source']
    tc = config['timecontrol']
    logging.debug(f'Arguments|{config}')
    val_src, val_tc = validate_args(src, tc)

    conn_str = get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)

    srcid, tcid = convert_names(conn, src, tc)
    val_srcid, val_tcid = convert_names(conn, val_src, val_tc)

    recalc = False
    param_dict = get_parameters(conn, val_srcid, val_tcid, recalc)

    csr = conn.cursor()
    sql_del = f'DELETE FROM stat.EvalDistributions WHERE SourceID = {srcid} AND TimeControlID = {tcid}'
    logging.debug(f'Delete query|{sql_del}')
    csr.execute(sql_del)
    conn.commit()

    m = param_dict['Mean']
    sd = param_dict['StandardDeviation']
    pdf_f = sd*np.sqrt(2*np.pi)

    rng = np.linspace(start=-15, stop=15, num=3001)
    for x in rng:
        pdf_val = NormalDist(mu=m, sigma=sd).pdf(x)*pdf_f  # this extra factor is to force f(0) = 1
        cdf_val = NormalDist(mu=m, sigma=sd).cdf(x)

        sql_cmd = 'INSERT INTO stat.EvalDistributions (SourceID, TimeControlID, Evaluation, PDF, CDF) '
        sql_cmd = sql_cmd + f'VALUES ({srcid}, {tcid}, {x}, {pdf_val}, {cdf_val})'
        logging.debug(f'Insert query|{sql_cmd}')
        csr.execute(sql_cmd)
        conn.commit()

    conn.close()


if __name__ == '__main__':
    main()
