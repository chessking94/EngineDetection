import argparse
import logging
import os
from pathlib import Path

from automation import misc
import sqlalchemy as sa

import classes as c

# TODO: Is there a way to make this more efficient? i.e. fewer queries and do calculations in-script?
# TODO: Review delete process when customizing parameters
# TODO: Look into adding covariance table write for Evaluation

CONFIG_FILE = os.path.join(Path(os.path.dirname(__file__)).parents[1], 'config.json')


def validate_args(config):
    if config['agg'] not in c.AGG_CHOICES:
        logging.critical(f"Invalid aggregation level|{config['agg']}")
        raise SystemExit

    # convert singletons to lists
    list_checks = ['src', 'fld', 'timecontrol', 'rating', 'evalgroup', 'color']
    for i in list_checks:
        if not isinstance(config[i], list) and config[i] is not None:
            config[i] = [config[i]]

    # modifiers
    if config['agg'] == 'Evaluation':
        rmv_list = ['T1', 'T2', 'T3', 'T4', 'T5', 'SDCPL', 'WinProbabilityLost', 'ScSDCPL', 'EvaluationGroupComparison']
        config['fld'] = [e for e in config['fld'] if e not in rmv_list]
        config['evalgroup'] = c.EVALGROUP_CHOICES if not config['evalgroup'] else config['evalgroup']
    elif config['agg'] == 'Event':
        if 'Lichess' in config['src']:
            logging.warning('Lichess requested for Event aggregation, removed as not developed')
            config['src'].remove('Lichess')

    # persist dictionary
    keys = ['fld', 'timecontrol', 'rating', 'evalgroup', 'color']
    for k in keys:
        temp_dict = {}
        for src in config['src']:
            temp_dict[src] = config[k]
        config[k] = temp_dict

    # update dictionary
    if 'Control' in config['src']:
        rmv_tc = ['Bullet', 'Blitz', 'Rapid']
        config['timecontrol']['Control'] = [e for e in config['timecontrol']['Control'] if e not in rmv_tc]
        config['rating']['Control'] = [e for e in config['rating']['Control'] if e < 2900]

    if 'Lichess' in config['src']:
        config['rating']['Lichess'] = [e for e in config['rating']['Lichess'] if e >= 2200]

    return config


def main():
    logging.basicConfig(
        format='%(funcName)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )

    vrs_num = '3.0'
    parser = argparse.ArgumentParser(
        description='Statistics Aggregator',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        usage=argparse.SUPPRESS
    )
    parser.add_argument(
        '-v', '--version',
        action='version',
        version='%(prog)s ' + vrs_num
    )
    parser.add_argument(
        '-a', '--agg',
        default='Event',
        choices=c.AGG_CHOICES,
        help='Aggregation level'
    )
    parser.add_argument(
        '-s', '--src',
        default=c.SRC_CHOICES,
        choices=c.SRC_CHOICES,
        help='Data source'
    )
    parser.add_argument(
        '-f', '--fld',
        default=c.FLD_CHOICES,
        choices=c.FLD_CHOICES,
        help='Statistic field'
    )
    parser.add_argument(
        '-t', '--timecontrol',
        default=c.TIMECONTROL_CHOICES,
        choices=c.TIMECONTROL_CHOICES,
        help='Time control'
    )
    parser.add_argument(
        '-r', '--rating',
        default=c.RATING_CHOICES,
        choices=c.RATING_CHOICES,
        help='Rating group to nearest hundred'
    )
    parser.add_argument(
        '-e', '--evalgroup',
        choices=c.EVALGROUP_CHOICES,
        help='Evaluation group'
    )
    parser.add_argument(
        '-c', '--color',
        default=c.COLOR_CHOICES,
        choices=c.COLOR_CHOICES,
        help='Color'
    )

    args = parser.parse_args()
    config = vars(args)
    data = validate_args(config)
    logging.debug(f'Arguments|{data}')

    conn_str = misc.get_config('connectionString_chessDB', CONFIG_FILE)
    connection_url = sa.engine.URL.create(
        drivername='mssql+pyodbc',
        query={"odbc_connect": conn_str}
    )
    engine = sa.create_engine(connection_url)

    agg = data['agg']
    for src in data['src']:
        fld = data['fld'][src]
        timecontrol = data['timecontrol'][src]
        rating = data['rating'][src]
        evalgroup = data['evalgroup'][src]
        color = data['color'][src]

        with c.aggregator(engine, agg, src, fld, timecontrol, rating, evalgroup, color) as req:
            if agg == 'Evaluation':
                req.evaluation()
            elif agg == 'Event':
                req.event()
            elif agg == 'Game':
                req.game()

    engine.dispose()


if __name__ == '__main__':
    main()
