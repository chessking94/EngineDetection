import logging
import os

import pyodbc as sql

from func import get_conf, get_config, parse_stats
from queries import get_evid, get_plid, get_srcid
import sections

# TODO: Populate stat.StatisticsSummary for Personal and PersonalOnline sources? Would likely just be a SQL Server thing to copy existing data
# TODO: Consider switching Mahalanobis calculation from (T1, ScACPL, Score) to (EVM, Moves, ScACPL, Score)


def main():
    logging.basicConfig(
        format='%(asctime)s\t%(funcName)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )

    config_path = os.path.dirname(__file__)
    for _ in range(2):
        config_path = os.path.dirname(config_path)

    conn_str = get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)

    src = get_config(config_path, 'reportSource')
    srcid = get_srcid(conn, src)
    rpt = get_config(config_path, 'reportType')

    engine_name = get_config(config_path, 'engineName')
    depth = get_config(config_path, 'depth')

    max_eval = get_config(config_path, 'maxEval')

    compare_stats = get_config(config_path, 'compareStats')
    compare_stats = parse_stats(compare_stats, conn)

    if rpt == 'Event':
        ev = get_config(config_path, 'eventName')
        evid = get_evid(conn, srcid, ev)
        full_name, plid, start_date, end_date = ['', ''], '', '', ''
    elif rpt == 'Player':
        ev, evid = '', ''
        first_name, last_name = get_config(config_path, 'firstName'), get_config(config_path, 'lastName')
        full_name = [first_name, last_name]
        plid = get_plid(conn, srcid, last_name, first_name)
        start_date, end_date = get_config(config_path, 'startDate'), get_config(config_path, 'endDate')

    report_path = get_config(config_path, 'reportPath')
    if rpt == 'Event':
        report_name = ev.replace("/", "").replace("\\", "") + '_report.txt'
    elif rpt == 'Player':
        report_name = f'{last_name}{first_name}_report.txt'
    else:
        logging.critical(f'Invalid report type|{rpt}')
        raise SystemExit

    report_full = os.path.join(report_path, report_name)

    max_eval = sections.update_maxeval(conn, max_eval)

    with open(report_full, 'w') as rf:
        g = sections.general(rf, compare_stats)
        r = sections.report(rf, compare_stats, rpt, conn, evid, plid, start_date, end_date)

        g.header_type(rpt, conn, ev, full_name, start_date, end_date)
        g.header_info(engine_name, depth)
        g.scoring_desc(conn)
        r.key_stats()
        g.player_key()
        r.player_summary()
        g.game_key()
        r.game_traces()

    conn.close()


if __name__ == '__main__':
    main()
