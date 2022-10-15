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
        # TODO: Get engine and depth values from DB somewhere?
        engine_name = get_config(config_path, 'engineName')
        depth = get_config(config_path, 'depth')
    else:
        pgn_name = get_config(config_path, 'pgnName')
        engine_name = get_config(config_path, 'engineName')
        depth = get_config(config_path, 'depth')

    if rpt == 'Event':
        ev = get_config(config_path, 'eventName')
    elif rpt == 'Player':
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

    # headers
    with open(report_full, 'w') as rf:
        sections.header_type(rf, rpt, conn, ev, full_name, start_date, end_date)
        sections.header_info(rf, pgn_name, engine_name, depth)
        sections.scoring_desc(rf, conn)

        if rpt == 'Event':
            sections.key_stats(rf, conn, ev, None, None, None)
            sections.player_key(rf)
            sections.player_summary(rf, conn, ev, None, None, None)
            sections.game_key(rf)
            sections.game_traces(rf, conn, ev, None, None, None)
        elif rpt == 'Player':
            sections.key_stats(rf, conn, None, full_name, start_date, end_date)
            sections.player_key(rf)
            sections.player_summary(rf, conn, None, full_name, start_date, end_date)
            sections.game_key(rf)
            sections.game_traces(rf, conn, None, full_name, start_date, end_date)

    conn.close()


if __name__ == '__main__':
    main()
