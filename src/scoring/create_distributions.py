import argparse
import json
import logging
from statistics import NormalDist

import numpy as np
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

    lc = {
        'Classical': {'Mean': 0, 'StDev': 4},
        'Rapid': {'Mean': 0, 'StDev': 4.5}
    }

    ctrl = {
        'Classical': {'Mean': 0, 'StDev': 2.4},
        'Correspondence': {'Mean': 0, 'StDev': np.exp(1)}
    }

    dist_dict = {'Lichess': lc, 'Control': ctrl}

    conn_str = get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)
    csr = conn.cursor()

    sql_del = f"DELETE FROM EvalDistributions WHERE Source = '{src}' AND TimeControlType = '{tc}'"
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

        sql_cmd = 'INSERT INTO EvalDistributions (Source, TimeControlType, Eval, PDF, CDF) '
        sql_cmd = sql_cmd + f"VALUES ('{src}', '{tc}', {x}, {pdf_val}, {cdf_val})"
        logging.debug(f'Delete query|{sql_cmd}')
        csr.execute(sql_cmd)
        conn.commit()

    conn.close()


if __name__ == '__main__':
    main()