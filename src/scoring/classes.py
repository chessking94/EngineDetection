import logging

import numpy as np
import pandas as pd
# import sqlalchemy as sa

import queries as q

NL = '\n'
AGG_CHOICES = ['Game', 'Event', 'Evaluation']
SRC_CHOICES = ['Control', 'Lichess']
FLD_CHOICES = ['T1', 'T2', 'T3', 'T4', 'T5', 'ACPL', 'SDCPL', 'WinProbabilityLost', 'ScACPL', 'ScSDCPL', 'EvaluationGroupComparison']
TIMECONTROL_CHOICES = ['Bullet', 'Blitz', 'Rapid', 'Classical', 'Correspondence']
RATING_CHOICES = [100*i for i in range(34)]
EVALGROUP_CHOICES = [i+1 for i in range(11)]
COLOR_CHOICES = ['White', 'Black']


class aggregator:
    def __init__(self, engine, agg, src, fld, timecontrol, rating, evalgroup, color):
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

    def aggregate_evals(self, fld, tctype, rating, evalgroup, color):
        qry_text = q.eval_qry(fld, self.src, tctype, rating, evalgroup, color)
        logging.debug(f"Select query|{qry_text.replace(NL, ' ')}")
        data_np = pd.read_sql(qry_text, self.engine).to_numpy()
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
            av = 'NULL'
            sd = 'NULL'
            lower_pcnt = 'NULL'
            qtr1 = 'NULL'
            qtr2 = 'NULL'
            qtr3 = 'NULL'
            upper_pcnt = 'NULL'
            mn = 'NULL'
            mx = 'NULL'

        return [ct, av, sd, mn, lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt, mx]

    def aggregate_event(self, fld, tcid, rating):
        qry_text = q.event_qry(self.src, tcid, rating)
        logging.debug(f"Select query|{qry_text.replace(NL, ' ')}")
        data = pd.read_sql(qry_text, self.engine)
        if len(data) > 0:
            data_arr = data[fld]
            ct = data_arr.count()
        else:
            ct = 0
        if ct > 0:
            av = np.mean(data_arr)
            sd = np.std(data_arr)
            mn = np.nanmin(data_arr)
            mx = np.nanmax(data_arr)

            ci_max = 100 - self.ci_min
            lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt = np.nanpercentile(data_arr, [self.ci_min, 25, 50, 75, ci_max])

            conn = self.engine.connect().connection
            csr = conn.cursor()
            dfcov = data.cov()  # throws a debug error when len(data) = 1, ignoring for now
            stats = dfcov.columns
            for r in stats:
                m1 = self.fld_dict[r]
                for c in stats:
                    cov = dfcov[r][c]
                    cov = 0 if np.isnan(cov) else cov
                    m2 = self.fld_dict[c]
                    if not q.check_cov(self.engine, self.src, self.agg, rating, tcid, 0, 0, m1, m2):
                        sql_cmd = 'INSERT INTO ChessWarehouse.stat.Covariances (SourceID, AggregationID, RatingID, TimeControlID, ColorID, '
                        sql_cmd = sql_cmd + 'EvaluationGroupID, MeasurementID1, MeasurementID2, Covariance) VALUES ('
                        sql_cmd = sql_cmd + f'{self.src}, {self.agg}, {rating}, {tcid}, 0, 0, {m1}, {m2}, {cov})'
                        logging.debug(f"Insert query|{sql_cmd.replace(NL, ' ')}")
                        csr.execute(sql_cmd)
                        conn.commit()
            conn.close()
        else:
            av = 'NULL'
            sd = 'NULL'
            mn = 'NULL'
            lower_pcnt = 'NULL'
            qtr1 = 'NULL'
            qtr2 = 'NULL'
            qtr3 = 'NULL'
            upper_pcnt = 'NULL'
            mx = 'NULL'

        return [ct, av, sd, mn, lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt, mx]

    def aggregate_game(self, fld, tcid, rating, colorid):
        qry_text = q.game_qry(self.src, tcid, rating, colorid)
        logging.debug(f"Select query|{qry_text.replace(NL, ' ')}")
        data = pd.read_sql(qry_text, self.engine)
        if len(data) > 0:
            data_arr = data[fld]
            ct = data_arr.count()
        else:
            ct = 0
        if ct > 0:
            av = np.mean(data_arr)
            sd = np.std(data_arr)
            mn = np.nanmin(data_arr)
            mx = np.nanmax(data_arr)

            ci_max = 100 - self.ci_min
            lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt = np.nanpercentile(data_arr, [self.ci_min, 25, 50, 75, ci_max])

            score_list = ['WinProbabilityLost', 'EvaluationGroupComparison']
            if fld in score_list and qtr3 > 100:
                qtr3 = 100
            if fld in score_list and upper_pcnt > 100:
                upper_pcnt = 100

            conn = self.engine.connect().connection
            csr = conn.cursor()
            dfcov = data.cov()
            stats = dfcov.columns
            for r in stats:
                m1 = self.fld_dict[r]
                for c in stats:
                    cov = dfcov[r][c]
                    cov = 0 if np.isnan(cov) else cov
                    m2 = self.fld_dict[c]
                    if not q.check_cov(self.engine, self.src, self.agg, rating, tcid, colorid, 0, m1, m2):
                        sql_cmd = 'INSERT INTO ChessWarehouse.stat.Covariances (SourceID, AggregationID, RatingID, TimeControlID, ColorID, '
                        sql_cmd = sql_cmd + 'EvaluationGroupID, MeasurementID1, MeasurementID2, Covariance) VALUES ('
                        sql_cmd = sql_cmd + f'{self.src}, {self.agg}, {rating}, {tcid}, {colorid}, 0, {m1}, {m2}, {cov})'
                        logging.debug(f"Insert query|{sql_cmd.replace(NL, ' ')}")
                        csr.execute(sql_cmd)
                        conn.commit()
            conn.close()
        else:
            av = 'NULL'
            sd = 'NULL'
            mn = 'NULL'
            lower_pcnt = 'NULL'
            qtr1 = 'NULL'
            qtr2 = 'NULL'
            qtr3 = 'NULL'
            upper_pcnt = 'NULL'
            mx = 'NULL'

        return [ct, av, sd, mn, lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt, mx]

    def delete_cov(self):
        sql_del = f'''
DELETE FROM ChessWarehouse.stat.Covariances
WHERE SourceID = {self.src}
AND AggregationID = {self.agg}
'''
        logging.debug(f"Delete query|{sql_del.replace(NL, ' ')}")
        return sql_del

    def delete_stats(self):
        sql_del = f'''
DELETE ss
FROM ChessWarehouse.stat.StatisticsSummary ss
JOIN ChessWarehouse.dim.Measurements m ON ss.MeasurementID = m.MeasurementID
JOIN ChessWarehouse.dim.TimeControls tc ON ss.TimeControlID = tc.TimeControlID
JOIN ChessWarehouse.dim.Colors c ON ss.ColorID = c.ColorID
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

        logging.debug(f"Delete query|{sql_del.replace(NL, ' ')}")
        return sql_del

    def evaluation(self):
        conn = self.engine.connect().connection
        csr = conn.cursor()

        sql_cmd = self.delete_stats()
        logging.debug(f"Delete query|{sql_cmd.replace(NL, ' ')}")
        csr.execute(sql_cmd)
        conn.commit()

        for fld in self.fld:
            fldid = q.get_fldid(self.engine, fld)
            for rating in self.rating:
                for tctype in self.timecontrol:
                    tcid = q.get_tcid(self.engine, tctype)
                    for color in self.color:
                        colorid = q.get_colorid(self.engine, color)
                        for evalgroup in self.evalgroup:
                            ct, av, sd, mn, lower, qt1, qt2, qt3, upper, mx = self.aggregate_evals(fld, tcid, rating, evalgroup, colorid)
                            sql_cmd = 'INSERT INTO ChessWarehouse.stat.StatisticsSummary (SourceID, AggregationID, MeasurementID, RatingID, TimeControlID, ColorID, '
                            sql_cmd = sql_cmd + 'EvaluationGroupID, RecordCount, Average, StandardDeviation, MinValue, LowerPcnt, LowerQuartile, Median, '
                            sql_cmd = sql_cmd + 'UpperQuartile, UpperPcnt, MaxValue) '
                            sql_cmd = sql_cmd + f"VALUES ({self.src}, {self.agg}, {fldid}, {rating}, {tcid}, {colorid}, "
                            sql_cmd = sql_cmd + f"{evalgroup}, {ct}, {av}, {sd}, {mn}, {lower}, {qt1}, {qt2}, "
                            sql_cmd = sql_cmd + f"{qt3}, {upper}, {mx})"
                            logging.debug(f"Insert query|{sql_cmd.replace(NL, ' ')}")
                            csr.execute(sql_cmd)
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

        for fld in self.fld:
            fldid = q.get_fldid(self.engine, fld)
            for rating in self.rating:
                for tctype in self.timecontrol:
                    tcid = q.get_tcid(self.engine, tctype)
                    ct, av, sd, mn, lower, qt1, qt2, qt3, upper, mx = self.aggregate_event(fld, tcid, rating)
                    sql_cmd = 'INSERT INTO ChessWarehouse.stat.StatisticsSummary (SourceID, AggregationID, MeasurementID, RatingID, TimeControlID, ColorID, EvaluationGroupID, '
                    sql_cmd = sql_cmd + 'RecordCount, Average, StandardDeviation, MinValue, LowerPcnt, LowerQuartile, Median, UpperQuartile, UpperPcnt, MaxValue) '
                    sql_cmd = sql_cmd + f"VALUES ({self.src}, {self.agg}, {fldid}, {rating}, {tcid}, 0, 0, "
                    sql_cmd = sql_cmd + f"{ct}, {av}, {sd}, {mn}, {lower}, {qt1}, {qt2}, {qt3}, {upper}, {mx})"
                    logging.debug(f"Insert query|{sql_cmd.replace(NL, ' ')}")
                    csr.execute(sql_cmd)
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

        for fld in self.fld:
            fldid = q.get_fldid(self.engine, fld)
            for rating in self.rating:
                for tctype in self.timecontrol:
                    tcid = q.get_tcid(self.engine, tctype)
                    for color in self.color:
                        colorid = q.get_colorid(self.engine, color)
                        ct, av, sd, mn, lower, qt1, qt2, qt3, upper, mx = self.aggregate_game(fld, tcid, rating, colorid)
                        sql_cmd = 'INSERT INTO ChessWarehouse.stat.StatisticsSummary (SourceID, AggregationID, MeasurementID, RatingID, TimeControlID, ColorID, EvaluationGroupID, '
                        sql_cmd = sql_cmd + 'RecordCount, Average, StandardDeviation, MinValue, LowerPcnt, LowerQuartile, Median, '
                        sql_cmd = sql_cmd + 'UpperQuartile, UpperPcnt, MaxValue) '
                        sql_cmd = sql_cmd + f"VALUES ({self.src}, {self.agg}, {fldid}, {rating}, {tcid}, {colorid}, 0, "
                        sql_cmd = sql_cmd + f"{ct}, {av}, {sd}, {mn}, {lower}, {qt1}, {qt2}, "
                        sql_cmd = sql_cmd + f"{qt3}, {upper}, {mx})"
                        logging.debug(f"Insert query|{sql_cmd.replace(NL, ' ')}")
                        csr.execute(sql_cmd)
                        conn.commit()
                        logging.info(f'Done with {self.src}|{fld}|{rating}|{tctype}|{color}')
        conn.close()
