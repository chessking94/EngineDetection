# import argparse
import logging
import os

import pyodbc as sql

from func import get_conf, get_config
import sections

RPT_CHOICES = ['Event', 'Game', 'Player']

# TODO: Create slightly different reports for individual games and custom player datasets
# TODO: Add CLI argument ability
# TODO: Performance rating (or +/-) based on ROI
# TODO: Scaled ACPL values a la Regan
# TODO: More complete ROI that includes more than just the score value
# TODO: Some kind of clear indicator if a performance is out of the ordinary, i.e. >= 80 ROI


def main():
    logging.basicConfig(
        format='%(funcName)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )

    # vrs_num = '1.0'
    # parser = argparse.ArgumentParser(
    #     description='Evalation Distribution Calculator',
    #     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    #     usage=argparse.SUPPRESS
    # )
    # parser.add_argument(
    #     '-v', '--version',
    #     action='version',
    #     version='%(prog)s ' + vrs_num
    # )
    # parser.add_argument(
    #     '-r', '--report',
    #     default='Event',
    #     choices=RPT_CHOICES,
    #     help='Report Type'
    # )

    # args = parser.parse_args()
    # config = vars(args)
    # rpt = config['report']
    # logging.debug(f'Arguments|{config}')

    config_path = os.path.dirname(__file__)
    for _ in range(2):
        config_path = os.path.dirname(config_path)

    rpt = get_config(config_path, 'reportType')

    db = get_config(config_path, 'useDatabase')
    if db:
        pgn_name = None
        # engine_name = 'N/A'
        # depth = 'N/A'
        engine_name = get_config(config_path, 'engineName')
        depth = get_config(config_path, 'depth')
    else:
        pgn_name = get_config(config_path, 'pgnName')
        engine_name = get_config(config_path, 'engineName')
        depth = get_config(config_path, 'depth')

    ev = 'Thursday Knighter I 2020 PREMIER'
    report_path = get_config(config_path, 'reportPath')
    report_name = f'{ev}_report.txt'
    report_full = os.path.join(report_path, report_name)

    conn_str = get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)

    # headers
    with open(report_full, 'w') as rf:
        sections.header_type(rf, rpt, conn, ev)
        sections.header_info(rf, pgn_name, engine_name, depth)
        sections.scoring_desc(rf, conn)

        if rpt == 'Event':
            sections.event_stats(rf, conn, ev)
            sections.player_key(rf)
            sections.event_playersummary(rf, conn, ev)
            sections.game_key(rf)
            sections.event_games(rf, conn, ev)
        # elif rpt == 'Game':
        #     pass
        # elif rpt == 'Player':
        #     pass
        else:
            logging.critical(f'Invalid report type|{rpt}')

    conn.close()


if __name__ == '__main__':
    main()
