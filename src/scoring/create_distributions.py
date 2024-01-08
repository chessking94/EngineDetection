import argparse
import json
import logging
from statistics import NormalDist

import numpy as np
import pandas as pd
import pyodbc as sql
from scipy import stats, optimize as opt

import queries as q

SRC_CHOICES = ['Personal', 'PersonalOnline', 'Control', 'Lichess', 'Test']
TC_CHOICES = ['Ultrabullet', 'Bullet', 'Blitz', 'Rapid', 'Classical', 'Correspondence']
MDL_CHOICES = ['Evaluation', 'CP_Loss']
RATING_CHOICES = [str(100*i) for i in range(34)]
EVALGROUP_CHOICES = [str(i+1) for i in range(11)]

# TODO: Convert some or all of this into a class


def convert_names(conn, src, tc):
    qry_text = f"""
SELECT
SourceID,
TimeControlID
FROM ChessWarehouse.dim.Sources s
CROSS JOIN ChessWarehouse.dim.TimeControls t
WHERE s.SourceName = '{src}'
AND t.TimeControlName = '{tc}'
"""
    logging.debug(f'Select query|{qry_text}')
    return pd.read_sql(qry_text, conn).values[0].tolist()


def get_conf(key):
    fname = r'C:\Users\eehunt\Repository\confidential.json'
    with open(fname, 'r') as t:
        key_data = json.load(t)
    val = key_data.get(key)
    return val


def get_curr_parameters(conn, mdl, srcid, tcid, ratingid, egid):
    if mdl == 'Evaluation':
        qry_text = f'''
SELECT
Mean,
StandardDeviation

FROM ChessWarehouse.stat.EvalDistributionParameters

WHERE SourceID = {srcid}
AND TimeControlID = {tcid}
'''
    elif mdl == 'CP_Loss':
        qry_text = f'''
SELECT
Alpha,
Beta

FROM ChessWarehouse.stat.CPLossDistributionParameters

WHERE SourceID = {srcid}
AND TimeControlID = {tcid}
AND RatingID = {ratingid}
AND EvaluationGroupID = {egid}
'''
    df = pd.read_sql(qry_text, conn)
    if len(df) == 0:
        df = [None, None]
    else:
        df = df.to_numpy()[0]
    return df


def get_distid(conn, disttype):
    qry = f"""
SELECT
DistributionID

FROM ChessWarehouse.stat.DistributionTypes

WHERE DistributionType = '{disttype}'
"""
    idval = int(pd.read_sql(qry, conn).values[0][0])
    return idval


def get_parameters(conn, mdl, srcid, tcid, ratingid, egid, recalc=False):
    if recalc:
        actual = observed_values(conn, mdl, srcid, tcid, ratingid, egid)
        if actual is None:
            params = [None, None]
        else:
            func, p_naught = get_optimizations(conn, mdl, srcid, tcid, ratingid, egid)
            # TODO: make sure params is returning [None, None] if no records are found
            if mdl == 'Evaluation':
                params = opt.curve_fit(f=func, xdata=actual['xdata'], ydata=actual['ydata'], p0=p_naught)[0]
            elif mdl == 'CP_Loss':
                params = p_naught  # TODO: Not sure why trying to fit a curve like this returns bogus values for a gamma function

            qry_text = q.parameter_stuff('Count', mdl, srcid, tcid, ratingid, egid, params)
            ct = len(pd.read_sql(qry_text, conn))

            csr = conn.cursor()
            if ct == 0:
                sql_ins = q.parameter_stuff('Insert', mdl, srcid, tcid, ratingid, egid, params)
                logging.debug(f'Insert query|{sql_ins}')
                csr.execute(sql_ins)
                conn.commit()
            else:
                sql_upd = q.parameter_stuff('Update', mdl, srcid, tcid, ratingid, egid, params)
                logging.debug(f'Update query|{sql_upd}')
                csr = conn.cursor()
                csr.execute(sql_upd)
                conn.commit()
    else:
        params = get_curr_parameters(conn, mdl, srcid, tcid, ratingid, egid)
    dict = {
        'p0': params[0],
        'p1': params[1]
    }
    return dict


def observed_values(conn, mdl, srcid, tcid, ratingid, egid):
    if mdl == 'Evaluation':
        qry_text = f'''
SELECT
ROUND(CAST(m.T1_Eval AS float), 1) AS xdata, --Evaluation
AVG(g.Result) AS ydata --Win_Percentage

FROM ChessWarehouse.lake.Moves m
JOIN ChessWarehouse.lake.Games g ON
    m.GameID = g.GameID
JOIN ChessWarehouse.dim.Sources s ON
    g.SourceID = s.SourceID
JOIN ChessWarehouse.dim.TimeControlDetail td ON
    g.TimeControlDetailID = td.TimeControlDetailID

WHERE g.SourceID = {srcid}
AND td.TimeControlID = {tcid}
AND m.T1_Eval_POV BETWEEN -3 AND 3

GROUP BY
ROUND(CAST(m.T1_Eval AS float), 1)
'''
    elif mdl == 'CP_Loss':
        qry_text = f"""
SELECT
100*CP_Loss AS xdata, --CP_Loss
COUNT(m.MoveNumber) AS Move_Count

FROM ChessWarehouse.lake.Moves m
JOIN ChessWarehouse.lake.Games g ON
    m.GameID = g.GameID
JOIN ChessWarehouse.dim.Colors c ON
    m.ColorID = c.ColorID
JOIN ChessWarehouse.dim.Sources s ON
    g.SourceID = s.SourceID
JOIN ChessWarehouse.dim.TimeControlDetail td ON
    g.TimeControlDetailID = td.TimeControlDetailID
JOIN ChessWarehouse.dim.Ratings r ON
    (CASE WHEN c.Color = 'White' THEN g.WhiteElo ELSE g.BlackElo END) >= r.RatingID AND
    (CASE WHEN c.Color = 'White' THEN g.WhiteElo ELSE g.BlackElo END) <= r.RatingUpperBound
JOIN ChessWarehouse.dim.EvaluationGroups eg ON
    m.T1_Eval_POV >= eg.LBound AND
    m.T1_Eval_POV <= eg.UBound

WHERE m.CP_Loss > 0
AND m.MoveScored = 1
AND (CASE WHEN c.Color = 'White' THEN g.WhiteBerserk ELSE g.BlackBerserk END) = 0
AND g.SourceID = {srcid}
AND td.TimeControlID = {tcid}
AND r.RatingID = {ratingid}
AND eg.EvaluationGroupID = {egid}

GROUP BY
100*CP_Loss
"""
    df = pd.read_sql(qry_text, conn)
    if len(df) == 0:
        return None
    else:
        if mdl == 'CP_Loss':
            col_total = df['Move_Count'].sum()
            df['ydata'] = df['Move_Count'] / col_total
        return df


def validate_args(mdl, src, tc, ratingid, egid):
    val_mdl = mdl
    val_src = src
    val_tc = tc
    val_ratingid = ratingid
    val_egid = egid
    if tc == 'Ultrabullet':
        logging.critical(f'Process not developed for {tc}, process terminated')
        raise SystemExit

    if src not in ['Control', 'Lichess']:
        val_src = 'Control'

    if tc in ['Bullet', 'Blitz', 'Rapid']:
        val_src = 'Lichess'

    return [val_mdl, val_src, val_tc, val_ratingid, val_egid]


def get_optimizations(conn, mdl, srcid, tcid, ratingid, egid):
    if mdl == 'Evaluation':
        func = stats.norm.cdf
        p_naught = [0, 1]
    elif mdl == 'CP_Loss':
        func = stats.gamma.pdf
        qry_text = f"""
SELECT
st.RecordCount,
st.Average*100 AS Average,
st.StandardDeviation*100 AS StandardDeviation

FROM ChessWarehouse.stat.StatisticsSummary st
JOIN ChessWarehouse.dim.Aggregations agg ON
    st.AggregationID = agg.AggregationID
JOIN ChessWarehouse.dim.Measurements m ON
    st.MeasurementID = m.MeasurementID

WHERE agg.AggregationName = 'Evaluation'
AND m.MeasurementName = 'ACPL'
AND st.RecordCount > 0
AND st.SourceID = {srcid}
AND st.TimeControlID = {tcid}
AND st.RatingID = {ratingid}
AND st.EvaluationGroupID = {egid}
"""
        df = pd.read_sql(qry_text, conn)
        col_total = df['RecordCount'].sum()
        if col_total > 0:
            df['Weights'] = df['RecordCount'] / col_total

            m = np.average(df['Average'], weights=df['Weights'])
            sd = np.average(df['StandardDeviation'], weights=df['Weights'])
            alpha = (m/sd)**2
            beta = m/(sd**2)
            p_naught = [alpha, beta]
        else:
            # TODO: Review null handling when there are no records, likely need it elsewhere as well
            p_naught = [1, 0]
    return [func, p_naught]


def main():
    logging.basicConfig(
        format='%(funcName)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )

    vrs_num = '3.0'
    parser = argparse.ArgumentParser(
        description='Distribution Calculator',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        usage=argparse.SUPPRESS
    )
    parser.add_argument(
        '-v', '--version',
        action='version',
        version='%(prog)s ' + vrs_num
    )
    parser.add_argument(
        '-m', '--model',
        default='CP_Loss',
        choices=MDL_CHOICES,
        help='Model Type'
    )
    parser.add_argument(
        '-s', '--source',
        default='Lichess',
        choices=SRC_CHOICES,
        help='Data source'
    )
    parser.add_argument(
        '-t', '--timecontrol',
        default='Blitz',
        choices=TC_CHOICES,
        help='Time control'
    )
    parser.add_argument(
        '-r', '--rating',
        default=2300,
        choices=RATING_CHOICES,
        help='Rating'
    )
    parser.add_argument(
        '-e', '--evalgroup',
        default=6,
        choices=EVALGROUP_CHOICES,
        help='Evaluation Group'
    )

    args = parser.parse_args()
    config = vars(args)
    mdl = config['model']
    src = config['source']
    tc = config['timecontrol']
    ratingid = config['rating']
    egid = config['evalgroup']
    logging.debug(f'Arguments|{config}')
    val_mdl, val_src, val_tc, val_ratingid, val_egid = validate_args(mdl, src, tc, ratingid, egid)

    conn_str = get_conf('SqlServerConnectionStringTrusted')
    conn = sql.connect(conn_str)

    srcid, tcid = convert_names(conn, src, tc)
    val_srcid, val_tcid = convert_names(conn, val_src, val_tc)  # These values may be different from prior line if validation changed them

    recalc = True
    param_dict = get_parameters(conn, val_mdl, val_srcid, val_tcid, val_ratingid, val_egid, recalc)
    if param_dict['p0'] is None:
        logging.critical(f'No data available! {mdl}|{src}|{tc}|{ratingid}|{egid}')
        raise SystemExit
    dist_dict = {
        'Evaluation': 'Normal',
        'CP_Loss': 'Gamma'
    }
    dist_id = get_distid(conn, dist_dict[val_mdl])

    if val_mdl == 'Evaluation':
        csr = conn.cursor()
        sql_del = f'DELETE FROM ChessWarehouse.stat.EvalDistributions WHERE SourceID = {srcid} AND TimeControlID = {tcid} AND DistributionID = {dist_id}'
        logging.debug(f'Delete query|{sql_del}')
        csr.execute(sql_del)
        conn.commit()

        p0 = param_dict['p0']
        p1 = param_dict['p1']
        pdf_f = p1*np.sqrt(2*np.pi)

        rng = np.linspace(start=-15, stop=15, num=3001)
        for x in rng:
            pdf_val = NormalDist(mu=p0, sigma=p1).pdf(x)*pdf_f  # this extra factor is to force f(0) = 1
            cdf_val = NormalDist(mu=p0, sigma=p1).cdf(x)

            sql_cmd = 'INSERT INTO ChessWarehouse.stat.EvalDistributions (SourceID, TimeControlID, Evaluation, DistributionID, PDF, CDF) '
            sql_cmd = sql_cmd + f'VALUES ({srcid}, {tcid}, {x}, {dist_id}, {pdf_val}, {cdf_val})'
            logging.debug(f'Insert query|{sql_cmd}')
            csr.execute(sql_cmd)
            conn.commit()
    elif mdl == 'CP_Loss':
        # TODO: rework this so it's not so duplicative
        csr = conn.cursor()
        sql_del = f'''
DELETE FROM ChessWarehouse.stat.CPLossDistributions
WHERE SourceID = {srcid}
AND TimeControlID = {tcid}
AND RatingID = {ratingid}
AND EvaluationGroupID = {egid}
AND DistributionID = {dist_id}
'''
        logging.debug(f'Delete query|{sql_del}')
        csr.execute(sql_del)
        conn.commit()

        p0 = param_dict['p0']
        p1 = param_dict['p1']
        loc = ((p0-1)*p1)/p0

        rng = np.linspace(start=1, stop=300, num=300)
        for x in rng:
            pdf_val = stats.gamma.pdf(x, p0, loc, 1/p1)
            cdf_val = stats.gamma.cdf(x, p0, loc, 1/p1)

            sql_cmd = 'INSERT INTO ChessWarehouse.stat.CPLossDistributions (SourceID, TimeControlID, RatingID, EvaluationGroupID, CP_Loss, DistributionID, PDF, CDF) '
            sql_cmd = sql_cmd + f'VALUES ({srcid}, {tcid}, {ratingid}, {egid}, {x/100}, {dist_id}, {pdf_val}, {cdf_val})'
            logging.debug(f'Insert query|{sql_cmd}')
            csr.execute(sql_cmd)
            conn.commit()

    conn.close()


if __name__ == '__main__':
    main()
