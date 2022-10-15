import logging
import os

import pyodbc as sql

from func import get_conf, get_config
import sections

RPT_CHOICES = ['Event', 'Player']  # , 'Game']

# TODO: Scaled ACPL values a la Regan? Maybe less important since I shifted the evaluation weight function so it's not symmetric about 0
# TODO: More complete ROI that includes more than just the score value
# TODO: Devise way to pass a PGN file and output the results
# TODO: Generalize the queries so can pull data from whatever tables I want
# TODO: Asterisks next to EVM, ACPL, SDCPL, and possibly score?


def main():
    logging.basicConfig(
        format='%(asctime)s\t%(funcName)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )

    config_path = os.path.dirname(__file__)
    for _ in range(2):
        config_path = os.path.dirname(config_path)

    rpt = get_config(config_path, 'reportType')

    db = get_config(config_path, 'useDatabase')
    if db:
        pgn_name = None
    else:
        pgn_name = get_config(config_path, 'pgnName')

    engine_name = get_config(config_path, 'engineName')
    depth = get_config(config_path, 'depth')

    if rpt == 'Event':
        ev = get_config(config_path, 'eventName')
        full_name = ['', '']
        start_date = ''
        end_date = ''
    elif rpt == 'Player':
        ev = ''
        first_name = get_config(config_path, 'firstName')
        last_name = get_config(config_path, 'lastName')
        full_name = [first_name, last_name]
        start_date = get_config(config_path, 'startDate')
        end_date = get_config(config_path, 'endDate')

    report_path = get_config(config_path, 'reportPath')
    if rpt == 'Event':
        report_name = f'{ev}_report.txt'
    elif rpt == 'Player':
        report_name = f'{last_name}{first_name}_report.txt'
    else:
        logging.critical(f'Invalid report type|{rpt}')
        raise SystemExit

    report_full = os.path.join(report_path, report_name)

    conn_str = get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)

    with open(report_full, 'w') as rf:
        g = sections.general(rf)
        r = sections.report(rf, rpt, conn, ev, full_name, start_date, end_date)

        g.header_type(rpt, conn, ev, full_name, start_date, end_date)
        g.header_info(pgn_name, engine_name, depth)
        g.scoring_desc(conn)
        r.key_stats()
        g.player_key()
        r.player_summary()
        g.game_key()
        r.game_traces()

    conn.close()


if __name__ == '__main__':
    main()
