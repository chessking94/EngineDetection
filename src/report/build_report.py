# import argparse
import logging
import os

import pyodbc as sql

from func import get_conf, get_config
import sections

RPT_CHOICES = ['Event', 'Player']  # , 'Game']

# TODO: Scaled ACPL values a la Regan? Maybe less important since I shifted the evaluation weight function so it's not symmetric about 0
# TODO: More complete ROI that includes more than just the score value
# TODO: Create slightly different reports for individual games and custom player datasets
# TODO: Add CLI argument ability
# TODO: Devise way to pass a PGN file and output the results
# TODO: Generalize the queries so can pull data from whatever tables I want
# TODO: May want to revisit hard-coding ROI >= 70 as the determinating factoring an abnormal performance, consider using something more scientific


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

    ev = '81st Tata Steel GpA'
    pl = ['Magnus', 'Carlsen']  # first, last
    sdate = '01/01/2019'
    edate = '12/31/2019'
    report_path = get_config(config_path, 'reportPath')
    if rpt == 'Event':
        report_name = f'{ev}_report.txt'
    elif rpt == 'Player':
        report_name = f'{pl[1]}{pl[0]}_report.txt'
    # elif rpt == 'Game':
    #     pass
    else:
        logging.critical(f'Invalid report type|{rpt}')
        raise SystemExit

    report_full = os.path.join(report_path, report_name)

    conn_str = get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)

    # headers
    with open(report_full, 'w') as rf:
        sections.header_type(rf, rpt, conn, ev, pl, sdate, edate)
        sections.header_info(rf, pgn_name, engine_name, depth)
        sections.scoring_desc(rf, conn)

        if rpt == 'Event':
            sections.key_stats(rf, conn, ev, None, None, None)
            sections.player_key(rf)
            sections.player_summary(rf, conn, ev, None, None, None)
            sections.game_key(rf)
            sections.game_traces(rf, conn, ev, None, None, None)
        elif rpt == 'Player':
            sections.key_stats(rf, conn, None, pl, sdate, edate)
            sections.player_key(rf)
            sections.player_summary(rf, conn, None, pl, sdate, edate)
            sections.game_key(rf)
            sections.game_traces(rf, conn, None, pl, sdate, edate)
        # elif rpt == 'Game':
        #     pass

    conn.close()


if __name__ == '__main__':
    main()
