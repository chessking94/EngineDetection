import argparse
import logging

import pyodbc as sql

from classes import aggregator
import Queries as q

# TODO: Move the deletes to their own (class?) function
# TODO: Consider implementing case insensitivity for arguments

AGG_CHOICES = ['Evaluation', 'Event', 'Game']
SRC_CHOICES = ['Control', 'Lichess']
FLD_CHOICES = ['ACPL', 'SDCPL', 'T1', 'T2', 'T3', 'T4', 'T5', 'Score']
TIMECONTROL_CHOICES = ['Rapid', 'Classical', 'Correspondence']
RATING_CHOICES = [1200+100*i for i in range(22)]
EVALGROUP_CHOICES = [i+1 for i in range(9)]
COLOR_CHOICES = ['White', 'Black']


def validate_args(config):
    if config['agg'] not in AGG_CHOICES:
        logging.critical(f"Invalid aggregation level|{config['agg']}")
        raise SystemExit

    # modifiers
    if config['agg'] == 'Evaluation':
        rmv_list = ['SDCPL', 'Score']
        config['fld'] = [e for e in config['fld'] if e not in rmv_list]
        config['evalgroup'] = EVALGROUP_CHOICES if not config['evalgroup'] else config['evalgroup']
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
        rmv_tc = ['Rapid']
        config['timecontrol']['Control'] = [e for e in config['timecontrol']['Control'] if e not in rmv_tc]
        config['rating']['Control'] = [e for e in config['rating']['Control'] if e < 2900]

    if 'Lichess' in config['src']:
        rmv_tc = ['Correspondence']
        config['timecontrol']['Lichess'] = [e for e in config['timecontrol']['Lichess'] if e not in rmv_tc]
        config['rating']['Lichess'] = [e for e in config['rating']['Lichess'] if e >= 2200]

    return config


def main():
    logging.basicConfig(
        format='%(funcName)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )

    vrs_num = '2.0'
    parser = argparse.ArgumentParser(
        description='Control Statistic Aggregator',
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
        default='Evaluation',
        choices=AGG_CHOICES,
        help='Aggregation level'
    )
    parser.add_argument(
        '-s', '--src',
        default=SRC_CHOICES,
        choices=SRC_CHOICES,
        help='Data source'
    )
    parser.add_argument(
        '-f', '--fld',
        default=FLD_CHOICES,
        choices=FLD_CHOICES,
        help='Statistic field'
    )
    parser.add_argument(
        '-t', '--timecontrol',
        default=TIMECONTROL_CHOICES,
        choices=TIMECONTROL_CHOICES,
        help='Time control'
    )
    parser.add_argument(
        '-r', '--rating',
        default=[1200+100*i for i in range(22)],
        choices=RATING_CHOICES,
        help='Rating group to nearest hundred'
    )
    parser.add_argument(
        '-e', '--evalgroup',
        choices=EVALGROUP_CHOICES,
        help='Evaluation group'
    )
    parser.add_argument(
        '-c', '--color',
        default=COLOR_CHOICES,
        choices=COLOR_CHOICES,
        help='Color'
    )

    args = parser.parse_args()
    config = vars(args)
    data = validate_args(config)

    conn_str = q.get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)

    agg = data['agg']
    for src in data['src']:
        fld = data['fld'][src]
        timecontrol = data['timecontrol'][src]
        rating = data['rating'][src]
        evalgroup = data['evalgroup'][src]
        color = data['color'][src]

        req = aggregator(conn, agg, src, fld, timecontrol, rating, evalgroup, color)

        if agg == 'Evaluation':
            req.evaluation()
        elif agg == 'Event':
            req.event()
        elif agg == 'Game':
            req.game()

    conn.close()


if __name__ == '__main__':
    main()
