import logging
import os
from pathlib import Path

import sqlalchemy as sa
from Utilities_Python import misc

from func import parse_stats
from queries import get_evid, get_plid, get_srcid
import sections

CONFIG_FILE = os.path.join(Path(__file__).parents[1], 'config.json')

# TODO: Populate stat.StatisticsSummary for Personal and PersonalOnline sources? Would likely just be a SQL Server thing to copy existing data
# TODO: Consider switching Mahalanobis calculation from (T1, ScACPL, Score) to (EVM, Moves, ScACPL, Score)
# TODO: Make ROI_Equal be entirely on equal ground; T1, ScACPL, and Score. Currently it is just for Score


def main():
    logging.basicConfig(
        format='%(asctime)s\t%(funcName)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )

    conn_str = os.getenv('ConnectionStringOdbcRelease')
    connection_url = sa.engine.URL.create(
        drivername='mssql+pyodbc',
        query={"odbc_connect": conn_str}
    )
    engine = sa.create_engine(connection_url)

    src = misc.get_config('reportSource', CONFIG_FILE)
    srcid = get_srcid(engine, src)
    rpt = misc.get_config('reportType', CONFIG_FILE)

    engine_name = misc.get_config('engineName', CONFIG_FILE)
    depth = misc.get_config('depth', CONFIG_FILE)

    max_eval = misc.get_config('maxEval', CONFIG_FILE)

    compare_stats = misc.get_config('compareStats', CONFIG_FILE)
    compare_stats = parse_stats(compare_stats, engine)

    if rpt == 'Event':
        ev = misc.get_config('eventName', CONFIG_FILE)
        evid = get_evid(engine, srcid, ev)
        full_name, plid, start_date, end_date = ['', ''], '', '', ''
    elif rpt == 'Player':
        ev, evid = '', ''
        first_name, last_name = misc.get_config('firstName', CONFIG_FILE), misc.get_config('lastName', CONFIG_FILE)
        full_name = [first_name, last_name]
        plid = get_plid(engine, srcid, last_name, first_name)
        start_date, end_date = misc.get_config('startDate', CONFIG_FILE), misc.get_config('endDate', CONFIG_FILE)

    report_path = misc.get_config('reportPath', CONFIG_FILE)
    if rpt == 'Event':
        report_name = ev.replace('/', '').replace('\\', '') + '_report.txt'
    elif rpt == 'Player':
        report_name = f'{last_name}{first_name}_report.txt'
    else:
        logging.critical(f'Invalid report type|{rpt}')
        raise SystemExit

    report_full = os.path.join(report_path, report_name)

    max_eval = sections.update_maxeval(engine, max_eval)

    with open(report_full, 'w') as rf:
        g = sections.general(rf, compare_stats)
        r = sections.report(rf, compare_stats, rpt, engine, evid, plid, start_date, end_date)

        g.header_type(rpt, engine, ev, full_name, start_date, end_date)
        g.header_info(engine_name, depth)
        g.scoring_desc(engine)
        r.key_stats()
        g.player_key()
        r.player_summary()
        g.game_key()
        r.game_traces()

    engine.dispose()


if __name__ == '__main__':
    main()
