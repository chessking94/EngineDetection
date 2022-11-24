import argparse
import json
import logging
from statistics import NormalDist

import numpy as np
import pandas as pd
import pyodbc as sql

SRC_CHOICES = ['Personal', 'PersonalOnline', 'Control', 'Lichess', 'Test']
TC_CHOICES = ['Ultrabullet', 'Bullet', 'Blitz', 'Rapid', 'Classical', 'Correspondence']


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

    per = {
        # "UltraBullet": {'Mean': 0.00, 'StDev': 0.00},
        # "Bullet": {'Mean': 0.00, 'StDev': 0.00},
        # "Blitz": {'Mean': 0.00, 'StDev': 0.00},
        'Rapid': {'Mean': 0.11, 'StDev': 3.87},
        'Classical': {'Mean': 0.08, 'StDev': 2.43},
        # 'Correspondence': {'Mean': 0.00, 'StDev': 1.89}
    }

    perol = {
        # "UltraBullet": {'Mean': 0.00, 'StDev': 0.00},
        "Bullet": {'Mean': 0.1, 'StDev': 7.96},
        "Blitz": {'Mean': 0.07, 'StDev': 6.16},
        'Rapid': {'Mean': 0.11, 'StDev': 3.87},
        'Classical': {'Mean': 0.08, 'StDev': 2.43},
        'Correspondence': {'Mean': 0.00, 'StDev': 1.89}
    }

    ctrl = {
        # "UltraBullet": {'Mean': 0.00, 'StDev': 0.00},
        # "Bullet": {'Mean': 0.00, 'StDev': 0.00},
        # "Blitz": {'Mean': 0.00, 'StDev': 0.00},
        # "Rapid": {'Mean': 0.00, 'StDev': 0.00},
        'Classical': {'Mean': 0.08, 'StDev': 2.43},
        'Correspondence': {'Mean': 0.00, 'StDev': 1.89}
    }

    lc = {
        # "UltraBullet": {'Mean': 0.00, 'StDev': 0.00},
        "Bullet": {'Mean': 0.1, 'StDev': 7.96},
        "Blitz": {'Mean': 0.07, 'StDev': 6.16},
        'Rapid': {'Mean': 0.11, 'StDev': 3.87},
        'Classical': {'Mean': 0.14, 'StDev': 3.33},
        "Correspondence": {'Mean': 0.17, 'StDev': 2.59}
    }

    tst = {
        # "UltraBullet": {'Mean': 0.00, 'StDev': 0.00},
        "Bullet": {'Mean': 0.1, 'StDev': 7.96},
        "Blitz": {'Mean': 0.07, 'StDev': 6.16},
        'Rapid': {'Mean': 0.11, 'StDev': 3.87},
        'Classical': {'Mean': 0.08, 'StDev': 2.43},
        'Correspondence': {'Mean': 0.00, 'StDev': 1.89}
    }

    dist_dict = {'Personal': per, 'PersonalOnline': perol, 'Control': ctrl, 'Lichess': lc, 'Test': tst}

    conn_str = get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)

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
    srcid, tcid = pd.read_sql(qry_text, conn).values[0].tolist()

    csr = conn.cursor()
    sql_del = f'DELETE FROM stat.EvalDistributions WHERE SourceID = {srcid} AND TimeControlID = {tcid}'
    logging.debug(f'Delete query|{sql_del}')
    csr.execute(sql_del)
    conn.commit()

    m = dist_dict[src][tc]['Mean']
    sd = dist_dict[src][tc]['StDev']
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
