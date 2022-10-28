import argparse
import json
import logging
from statistics import NormalDist

import numpy as np
import pandas as pd
import pyodbc as sql

SRC_CHOICES = ['Control', 'Lichess']
TC_CHOICES = ['Rapid', 'Classical', 'Correspondence']


def get_conf(key):
    fname = r'C:\Users\eehunt\Repository\confidential.json'
    with open(fname, 'r') as t:
        key_data = json.load(t)
    val = key_data.get(key)
    return val


def main():
    logging.basicConfig(
        format='%(funcName)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )

    vrs_num = '1.0'
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
        default='Control',
        choices=SRC_CHOICES,
        help='Data source'
    )
    parser.add_argument(
        '-t', '--timecontrol',
        default='Classical',
        choices=TC_CHOICES,
        help='Time control'
    )

    args = parser.parse_args()
    config = vars(args)
    src = config['source']
    tc = config['timecontrol']
    logging.debug(f'Arguments|{config}')

    lc = {
        'Classical': {'Mean': 0.15, 'StDev': 3.88},
        'Rapid': {'Mean': 0.19, 'StDev': 4.47}
    }

    ctrl = {
        'Classical': {'Mean': 0.35, 'StDev': 1.85},
        'Correspondence': {'Mean': 0.34, 'StDev': 2.55}
    }

    dist_dict = {'Lichess': lc, 'Control': ctrl}

    conn_str = get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)

    qry_text = f"""
SELECT
SourceID,
TimeControlID
FROM ChessWarehouse.dim.Sources s
CROSS APPLY ChessWarehouse.dim.TimeControls t
WHERE s.SourceName = '{src}'
AND t.TimeControlName = '{tc}'
"""
    logging.debug(f'Select query|{qry_text}')
    srcid, tcid = pd.read_sql(qry_text, conn).values[0].tolist()

    csr = conn.cursor()
    sql_del = f'DELETE FROM ChessWarehouse.stat.EvalDistributions WHERE SourceID = {srcid} AND TimeControlID = {tcid}'
    logging.debug(f'Delete query|{sql_del}')
    csr.execute(sql_del)
    conn.commit()

    m = dist_dict[src][tc]['Mean']
    sd = dist_dict[src][tc]['StDev']
    pdf_f = sd*np.sqrt(2*np.pi)

    rng = np.linspace(start=-100, stop=100, num=20001)
    for x in rng:
        pdf_val = NormalDist(mu=m, sigma=sd).pdf(x)*pdf_f  # this extra factor is to force f(0) = 1
        cdf_val = NormalDist(mu=m, sigma=sd).cdf(x)

        sql_cmd = 'INSERT INTO ChessWarehouse.stat.EvalDistributions (SourceID, TimeControlID, Evaluation, PDF, CDF) '
        sql_cmd = sql_cmd + f'VALUES ({srcid}, {tcid}, {x}, {pdf_val}, {cdf_val})'
        logging.debug(f'Insert query|{sql_cmd}')
        csr.execute(sql_cmd)
        conn.commit()

    conn.close()


if __name__ == '__main__':
    main()
