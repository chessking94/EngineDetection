import argparse
import logging

import pyodbc as sql

import classes as c
import queries as q

# TODO: Review delete process when customizing paramters
# TODO: Look into adding covariance table write for Evaluation
# TODO: Considering looping through multiple sources


def validate_args(config):
    if config['agg'] not in c.AGG_CHOICES:
        logging.critical(f"Invalid aggregation level|{config['agg']}")
        raise SystemExit

    # modifiers
    if config['agg'] == 'Evaluation':
        rmv_list = ['T1', 'T2', 'T3', 'T4', 'T5', 'SDCPL', 'Score', 'ScSDCPL', 'ScoreEqual']
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

    conn_str = q.get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)

    agg = data['agg']
    for src in data['src']:
        fld = data['fld'][src]
        timecontrol = data['timecontrol'][src]
        rating = data['rating'][src]
        evalgroup = data['evalgroup'][src]
        color = data['color'][src]

        req = c.aggregator(conn, agg, src, fld, timecontrol, rating, evalgroup, color)

        if agg == 'Evaluation':
            req.evaluation()
        elif agg == 'Event':
            req.event()
        elif agg == 'Game':
            req.game()

    conn.close()


if __name__ == '__main__':
    main()
