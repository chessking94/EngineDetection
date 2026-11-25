import logging

import numpy as np
import pandas as pd
import sqlalchemy as sa

import queries as q

NL = '\n'
AGG_CHOICES = ['Game', 'Event', 'Evaluation']
SRC_CHOICES = ['Control', 'Lichess']
FLD_CHOICES = ['T1', 'T2', 'T3', 'T4', 'T5', 'ACPL', 'SDCPL', 'WinProbabilityLost', 'ScACPL', 'ScSDCPL', 'ContextualMoveQuality']
TIMECONTROL_CHOICES = ['Bullet', 'Blitz', 'Rapid', 'Classical', 'Correspondence']
RATING_CHOICES = [100*i for i in range(34)]
EVALGROUP_CHOICES = [i+1 for i in range(11)]
COLOR_CHOICES = ['White', 'Black']


class aggregator:
    def __init__(self, engine: sa.Engine, agg: str, src: str, fld: list[str], timecontrol: list[str], rating: list[int], evalgroup: list[int], color: list[str]):
        self.engine = engine
        self.agg = q.get_aggid(engine, agg)
        self.src = q.get_srcid(engine, src)
        self.fld = fld
        self.timecontrol = timecontrol
        self.rating = rating
        self.evalgroup = evalgroup
        self.color = color
        self.ci_min = 1
        self.fld_dict = q.get_flddict(engine)
        self.tcid_dict = q.get_tcdict(engine)
        self.colorid_dict = q.get_colordict(engine)

    def aggregate_evals(self, fld: str, data: pd.DataFrame):
        data_np = data[[fld]].to_numpy()
        if len(data_np) > 0:
            data_arr = np.concatenate(data_np)

            ct = len(data_arr)
            av = np.mean(data_arr)
            sd = np.std(data_arr)
            mn = np.nanmin(data_arr)
            mx = np.nanmax(data_arr)

            ci_max = 100 - self.ci_min
            lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt = np.nanpercentile(data_arr, [self.ci_min, 25, 50, 75, ci_max])
        else:
            ct = 0
            av = None
            sd = None
            lower_pcnt = None
            qtr1 = None
            qtr2 = None
            qtr3 = None
            upper_pcnt = None
            mn = None
            mx = None

        return [ct, av, sd, mn, lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt, mx]

    def aggregate_event(self, fld: str, data: pd.DataFrame):
        if len(data) > 0:
            data_arr = data[fld]
            ct = int(data_arr.count())
        else:
            ct = 0
        if ct > 0:
            av = np.mean(data_arr)
            sd = np.std(data_arr)
            mn = np.nanmin(data_arr)
            mx = np.nanmax(data_arr)

            ci_max = 100 - self.ci_min
            lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt = np.nanpercentile(data_arr, [self.ci_min, 25, 50, 75, ci_max])

        else:
            av = None
            sd = None
            mn = None
            lower_pcnt = None
            qtr1 = None
            qtr2 = None
            qtr3 = None
            upper_pcnt = None
            mx = None

        return [ct, av, sd, mn, lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt, mx]

    def aggregate_game(self, fld: str, data: pd.DataFrame):
        if len(data) > 0:
            data_arr = data[fld]
            ct = int(data_arr.count())
        else:
            ct = 0
        if ct > 0:
            av = np.mean(data_arr)
            sd = np.std(data_arr)
            mn = np.nanmin(data_arr)
            mx = np.nanmax(data_arr)

            ci_max = 100 - self.ci_min
            lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt = np.nanpercentile(data_arr, [self.ci_min, 25, 50, 75, ci_max])

            score_list = ['WinProbabilityLost', 'ContextualMoveQuality']
            if fld in score_list and qtr3 > 100:
                qtr3 = 100
            if fld in score_list and upper_pcnt > 100:
                upper_pcnt = 100

        else:
            av = None
            sd = None
            mn = None
            lower_pcnt = None
            qtr1 = None
            qtr2 = None
            qtr3 = None
            upper_pcnt = None
            mx = None

        return [ct, av, sd, mn, lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt, mx]

    def save_covariances(self, data: pd.DataFrame, rating: int, tcid: int, colorid: int):
        if data.empty:
            return

        conn = self.engine.connect().connection
        csr = conn.cursor()
        dfcov = data.cov()
        for r in dfcov.columns:
            m1 = self.fld_dict[r]
            for c in dfcov.columns:
                cov = dfcov[r][c]
                cov = 0 if np.isnan(cov) else cov
                m2 = self.fld_dict[c]
                if not q.check_cov(self.engine, self.src, self.agg, rating, tcid, colorid, 0, m1, m2):
                    sql_cmd = 'INSERT INTO ChessWarehouse.stat.Covariances (SourceID, AggregationID, RatingID, TimeControlID, ColorID, '
                    sql_cmd = sql_cmd + 'EvaluationGroupID, MeasurementID1, MeasurementID2, Covariance) '
                    sql_cmd = sql_cmd + 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
                    logging.debug(f"Insert query|{sql_cmd.replace(NL, ' ')}")
                    csr.execute(sql_cmd, [self.src, self.agg, rating, tcid, colorid, 0, m1, m2, cov])
                    conn.commit()
        conn.close()

    def delete_cov(self):
        sql_del = f'''
DELETE FROM ChessWarehouse.stat.Covariances
WHERE SourceID = {self.src}
AND AggregationID = {self.agg}
'''
        return sql_del

    def delete_stats(self):
        sql_del = f'''
DELETE ss
FROM ChessWarehouse.stat.StatisticsSummary AS ss
INNER JOIN ChessWarehouse.dim.Measurements AS m ON ss.MeasurementID = m.MeasurementID
INNER JOIN ChessWarehouse.dim.TimeControls AS tc ON ss.TimeControlID = tc.TimeControlID
INNER JOIN ChessWarehouse.dim.Colors AS c ON ss.ColorID = c.ColorID
WHERE ss.SourceID = {self.src}
AND ss.AggregationID = {self.agg}
'''

        if self.fld is not None:
            fld_list = ','.join(f"'{i}'" for i in self.fld)
            sql_del = sql_del + f'AND m.MeasurementName IN ({fld_list})' + NL

        if self.timecontrol is not None:
            timecontrol_list = ','.join(f"'{i}'" for i in self.timecontrol)
            sql_del = sql_del + f'AND tc.TimeControlName IN ({timecontrol_list})' + NL

        if self.rating is not None:
            rating_list = ','.join(str(i) for i in self.rating)
            sql_del = sql_del + f'AND ss.RatingID IN ({rating_list})' + NL

        if self.evalgroup is not None:
            evalgroup_list = ','.join(str(i) for i in self.evalgroup)
            sql_del = sql_del + f'AND ss.EvaluationGroupID IN ({evalgroup_list})' + NL

        if self.color is not None and len(self.color) < 2:
            color_list = ','.join(f"'{i}'" for i in self.color)
            sql_del = sql_del + f'AND c.Color IN ({color_list})' + NL

        return sql_del

    def evaluation(self):
        conn = self.engine.connect().connection
        csr = conn.cursor()

        sql_cmd = self.delete_stats()
        logging.debug(f"Delete query|{sql_cmd.replace(NL, ' ')}")
        csr.execute(sql_cmd)
        conn.commit()

        fields = ', '.join(self.fld)
        for rating in self.rating:
            for tctype in self.timecontrol:
                tcid = self.tcid_dict[tctype]
                for color in self.color:
                    colorid = self.colorid_dict[color]
                    for evalgroup in self.evalgroup:
                        qry_text = q.eval_qry(fields, self.src, tcid, rating, evalgroup, colorid)
                        logging.debug(f"Select query|{qry_text.replace(NL, ' ')}")
                        data = pd.read_sql(qry_text, self.engine)
                        for fld in self.fld:
                            fldid = self.fld_dict[fld]
                            ct, av, sd, mn, lower, qt1, qt2, qt3, upper, mx = self.aggregate_evals(fld, data)
                            sql_cmd = 'INSERT INTO ChessWarehouse.stat.StatisticsSummary (SourceID, AggregationID, MeasurementID, RatingID, TimeControlID, ColorID, '
                            sql_cmd = sql_cmd + 'EvaluationGroupID, RecordCount, Average, StandardDeviation, MinValue, LowerPcnt, LowerQuartile, Median, '
                            sql_cmd = sql_cmd + 'UpperQuartile, UpperPcnt, MaxValue) '
                            sql_cmd = sql_cmd + 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                            logging.debug(f"Insert query|{sql_cmd.replace(NL, ' ')}")
                            csr.execute(sql_cmd, [self.src, self.agg, fldid, rating, tcid, colorid, evalgroup, ct, av, sd, mn, lower, qt1, qt2, qt3, upper, mx])
                            conn.commit()
                            logging.info(f'Done with {self.src}|{fld}|{rating}|{evalgroup}|{color}|{tctype}')
        conn.close()

    def event(self):
        conn = self.engine.connect().connection
        csr = conn.cursor()

        sql_cmd = self.delete_stats()
        logging.debug(f"Delete query|{sql_cmd.replace(NL, ' ')}")
        csr.execute(sql_cmd)
        conn.commit()

        sql_cmd = self.delete_cov()
        logging.debug(f"Delete query|{sql_cmd.replace(NL, ' ')}")
        csr.execute(sql_cmd)
        conn.commit()

        for rating in self.rating:
            for tctype in self.timecontrol:
                tcid = self.tcid_dict[tctype]
                qry_text = q.event_qry(self.src, tcid, rating)
                logging.debug(f"Select query|{qry_text.replace(NL, ' ')}")
                data = pd.read_sql(qry_text, self.engine)
                summaries = [(fld, self.aggregate_event(fld, data)) for fld in self.fld]
                if any(summary[0] > 0 for _, summary in summaries):
                    self.save_covariances(data, rating, tcid, 0)
                for fld, summary in summaries:
                    fldid = self.fld_dict[fld]
                    ct, av, sd, mn, lower, qt1, qt2, qt3, upper, mx = summary
                    sql_cmd = 'INSERT INTO ChessWarehouse.stat.StatisticsSummary (SourceID, AggregationID, MeasurementID, RatingID, TimeControlID, ColorID, EvaluationGroupID, '
                    sql_cmd = sql_cmd + 'RecordCount, Average, StandardDeviation, MinValue, LowerPcnt, LowerQuartile, Median, UpperQuartile, UpperPcnt, MaxValue) '
                    sql_cmd = sql_cmd + 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                    logging.debug(f"Insert query|{sql_cmd.replace(NL, ' ')}")
                    csr.execute(sql_cmd, [self.src, self.agg, fldid, rating, tcid, 0, 0, ct, av, sd, mn, lower, qt1, qt2, qt3, upper, mx])
                    conn.commit()
                    logging.info(f'Done with {self.src}|{fld}|{rating}|{tctype}')
        conn.close()

    def game(self):
        conn = self.engine.connect().connection
        csr = conn.cursor()

        sql_cmd = self.delete_stats()
        logging.debug(f"Delete query|{sql_cmd.replace(NL, ' ')}")
        csr.execute(sql_cmd)
        conn.commit()

        sql_cmd = self.delete_cov()
        logging.debug(f"Delete query|{sql_cmd.replace(NL, ' ')}")
        csr.execute(sql_cmd)
        conn.commit()

        for rating in self.rating:
            for tctype in self.timecontrol:
                tcid = self.tcid_dict[tctype]
                for color in self.color:
                    colorid = self.colorid_dict[color]
                    qry_text = q.game_qry(self.src, tcid, rating, colorid)
                    logging.debug(f"Select query|{qry_text.replace(NL, ' ')}")
                    data = pd.read_sql(qry_text, self.engine)
                    summaries = [(fld, self.aggregate_game(fld, data)) for fld in self.fld]
                    if any(summary[0] > 0 for _, summary in summaries):
                        self.save_covariances(data, rating, tcid, colorid)
                    for fld, summary in summaries:
                        fldid = self.fld_dict[fld]
                        ct, av, sd, mn, lower, qt1, qt2, qt3, upper, mx = summary
                        sql_cmd = 'INSERT INTO ChessWarehouse.stat.StatisticsSummary (SourceID, AggregationID, MeasurementID, RatingID, TimeControlID, ColorID, EvaluationGroupID, '
                        sql_cmd = sql_cmd + 'RecordCount, Average, StandardDeviation, MinValue, LowerPcnt, LowerQuartile, Median, '
                        sql_cmd = sql_cmd + 'UpperQuartile, UpperPcnt, MaxValue) '
                        sql_cmd = sql_cmd + 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                        logging.debug(f"Insert query|{sql_cmd.replace(NL, ' ')}")
                        csr.execute(sql_cmd, [self.src, self.agg, fldid, rating, tcid, colorid, 0, ct, av, sd, mn, lower, qt1, qt2, qt3, upper, mx])
                        conn.commit()
                        logging.info(f'Done with {self.src}|{fld}|{rating}|{tctype}|{color}')
        conn.close()
