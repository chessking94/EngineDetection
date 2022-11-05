import logging

import numpy as np
import pandas as pd

import queries as q

NL = '\n'
AGG_CHOICES = ['Game', 'Event', 'Evaluation']
SRC_CHOICES = ['Control', 'Lichess']
FLD_CHOICES = ['T1', 'T2', 'T3', 'T4', 'T5', 'ACPL', 'SDCPL', 'Score', 'ScACPL', 'ScSDCPL']
TIMECONTROL_CHOICES = ['Rapid', 'Classical', 'Correspondence']
RATING_CHOICES = [1200+100*i for i in range(22)]
EVALGROUP_CHOICES = [i+1 for i in range(9)]
COLOR_CHOICES = ['White', 'Black']


class aggregator:
    def __init__(self, conn, agg, src, fld, timecontrol, rating, evalgroup, color):
        self.conn = conn
        self.agg = q.get_aggid(conn, agg)
        self.src = q.get_srcid(conn, src)
        self.fld = fld
        self.timecontrol = timecontrol
        self.rating = rating
        self.evalgroup = evalgroup
        self.color = color
        self.ci_min = 1
        self.fld_dict = q.get_flddict(conn)

    def aggregate_evals(self, fld, tctype, rating, evalgroup, color):
        qry_text = q.eval_qry(fld, self.src, tctype, rating, evalgroup, color)
        logging.debug(f"Select query|{qry_text.replace(NL, ' ')}")
        data_np = pd.read_sql(qry_text, self.conn).to_numpy()
        if len(data_np) > 0:
            data_arr = np.concatenate(data_np)

            ct = len(data_arr)
            av = np.mean(data_arr)
            sd = np.std(data_arr)
            mn = min(data_arr)
            mx = max(data_arr)

            ci_max = 100 - self.ci_min
            lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt = np.percentile(data_arr, [self.ci_min, 25, 50, 75, ci_max])
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
        data = pd.read_sql(qry_text, self.conn)
        if len(data) > 0:
            data_arr = data[fld]

            ct = len(data_arr)
            av = np.mean(data_arr)
            sd = np.std(data_arr)
            mn = min(data_arr)
            mx = max(data_arr)

            ci_max = 100 - self.ci_min
            lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt = np.percentile(data_arr, [self.ci_min, 25, 50, 75, ci_max])

            csr = self.conn.cursor()
            dfcov = data.cov()
            stats = dfcov.columns
            for r in stats:
                m1 = self.fld_dict[r]
                for c in stats:
                    cov = dfcov[r][c]
                    cov = 0 if np.isnan(cov) else cov
                    m2 = self.fld_dict[c]
                    if not q.check_cov(self.conn, self.src, self.agg, rating, tcid, 0, 0, m1, m2):
                        sql_cmd = 'INSERT INTO stat.Covariances (SourceID, AggregationID, RatingID, TimeControlID, ColorID, '
                        sql_cmd = sql_cmd + 'EvaluationGroupID, MeasurementID1, MeasurementID2, Covariance) VALUES ('
                        sql_cmd = sql_cmd + f'{self.src}, {self.agg}, {rating}, {tcid}, 0, 0, {m1}, {m2}, {cov})'
                        logging.debug(f"Insert query|{sql_cmd.replace(NL, ' ')}")
                        csr.execute(sql_cmd)
                        self.conn.commit()
        else:
            ct = 0
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
        data = pd.read_sql(qry_text, self.conn)
        if len(data) > 0:
            data_arr = data[fld]

            ct = len(data_arr)
            av = np.mean(data_arr)
            sd = np.std(data_arr)
            mn = min(data_arr)
            mx = max(data_arr)

            ci_max = 100 - self.ci_min
            lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt = np.percentile(data_arr, [self.ci_min, 25, 50, 75, ci_max])

            if fld == 'Score' and qtr3 > 100:
                qtr3 = 100
            if fld == 'Score' and upper_pcnt > 100:
                upper_pcnt = 100

            csr = self.conn.cursor()
            dfcov = data.cov()
            stats = dfcov.columns
            for r in stats:
                m1 = self.fld_dict[r]
                for c in stats:
                    cov = dfcov[r][c]
                    cov = 0 if np.isnan(cov) else cov
                    m2 = self.fld_dict[c]
                    if not q.check_cov(self.conn, self.src, self.agg, rating, tcid, colorid, 0, m1, m2):
                        sql_cmd = 'INSERT INTO stat.Covariances (SourceID, AggregationID, RatingID, TimeControlID, ColorID, '
                        sql_cmd = sql_cmd + 'EvaluationGroupID, MeasurementID1, MeasurementID2, Covariance) VALUES ('
                        sql_cmd = sql_cmd + f'{self.src}, {self.agg}, {rating}, {tcid}, {colorid}, 0, {m1}, {m2}, {cov})'
                        logging.debug(f"Insert query|{sql_cmd.replace(NL, ' ')}")
                        csr.execute(sql_cmd)
                        self.conn.commit()
        else:
            ct = 0
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
DELETE FROM stat.Covariances
WHERE SourceID = {self.src}
AND AggregationID = {self.agg}
'''
        logging.debug(f"Delete query|{sql_del.replace(NL, ' ')}")
        return sql_del

    def delete_stats(self):
        sql_del = f'''
DELETE FROM stat.StatisticsSummary
WHERE SourceID = {self.src}
AND AggregationID = {self.agg}
'''

        # if self.fld and len(self.fld) < len(FLD_CHOICES):
        #     fld_list = ','.join(f"'{i}'" for i in self.fld)
        #     sql_del = sql_del + f'AND Field IN ({fld_list})' + NL

        # if self.timecontrol and len(self.timecontrol) < len(TIMECONTROL_CHOICES):
        #     timecontrol_list = ','.join(f"'{i}'" for i in self.timecontrol)
        #     sql_del = sql_del + f'AND TimeControlType IN ({timecontrol_list})' + NL

        # if self.rating and len(self.rating) < len(RATING_CHOICES):
        #     rating_list = ','.join(str(i) for i in self.rating)
        #     sql_del = sql_del + f'AND Rating IN ({rating_list})' + NL

        # if self.evalgroup and len(self.evalgroup) < len(EVALGROUP_CHOICES):
        #     evalgroup_list = ','.join(str(i) for i in self.evalgroup)
        #     sql_del = sql_del + f'AND EvalGroup IN ({evalgroup_list})' + NL

        # if self.color and len(self.color) < len(COLOR_CHOICES):
        #     color_list = ','.join(f"'{i}'" for i in self.color)
        #     sql_del = sql_del + f'AND Color IN ({color_list})' + NL

        logging.debug(f"Delete query|{sql_del.replace(NL, ' ')}")
        return sql_del

    def evaluation(self):
        csr = self.conn.cursor()

        sql_cmd = self.delete_stats()
        csr.execute(sql_cmd)
        self.conn.commit()

        for fld in self.fld:
            fldid = q.get_fldid(self.conn, fld)
            for rating in self.rating:
                for tctype in self.timecontrol:
                    tcid = q.get_tcid(self.conn, tctype)
                    for color in self.color:
                        colorid = q.get_colorid(self.conn, color)
                        for evalgroup in self.evalgroup:
                            ct, av, sd, mn, lower, qt1, qt2, qt3, upper, mx = self.aggregate_evals(fld, tcid, rating, evalgroup, colorid)
                            sql_cmd = 'INSERT INTO stat.StatisticsSummary (SourceID, AggregationID, MeasurementID, RatingID, TimeControlID, ColorID, '
                            sql_cmd = sql_cmd + 'EvaluationGroupID, RecordCount, Average, StandardDeviation, MinValue, LowerPcnt, LowerQuartile, Median, '
                            sql_cmd = sql_cmd + 'UpperQuartile, UpperPcnt, MaxValue) '
                            sql_cmd = sql_cmd + f"VALUES ({self.src}, {self.agg}, {fldid}, {rating}, {tcid}, {colorid}, "
                            sql_cmd = sql_cmd + f"{evalgroup}, {ct}, {av}, {sd}, {mn}, {lower}, {qt1}, {qt2}, "
                            sql_cmd = sql_cmd + f"{qt3}, {upper}, {mx})"
                            logging.debug(f"Insert query|{sql_cmd.replace(NL, ' ')}")
                            csr.execute(sql_cmd)
                            self.conn.commit()
                            logging.info(f'Done with {self.src}|{fld}|{rating}|{evalgroup}|{color}|{tctype}')

    def event(self):
        csr = self.conn.cursor()

        sql_cmd = self.delete_stats()
        csr.execute(sql_cmd)
        self.conn.commit()

        sql_cmd = self.delete_cov()
        csr.execute(sql_cmd)
        self.conn.commit()

        for fld in self.fld:
            fldid = q.get_fldid(self.conn, fld)
            for rating in self.rating:
                for tctype in self.timecontrol:
                    tcid = q.get_tcid(self.conn, tctype)
                    ct, av, sd, mn, lower, qt1, qt2, qt3, upper, mx = self.aggregate_event(fld, tcid, rating)
                    sql_cmd = 'INSERT INTO stat.StatisticsSummary (SourceID, AggregationID, MeasurementID, RatingID, TimeControlID, ColorID, EvaluationGroupID, '
                    sql_cmd = sql_cmd + 'RecordCount, Average, StandardDeviation, MinValue, LowerPcnt, LowerQuartile, Median, UpperQuartile, UpperPcnt, MaxValue) '
                    sql_cmd = sql_cmd + f"VALUES ({self.src}, {self.agg}, {fldid}, {rating}, {tcid}, 0, 0, "
                    sql_cmd = sql_cmd + f"{ct}, {av}, {sd}, {mn}, {lower}, {qt1}, {qt2}, {qt3}, {upper}, {mx})"
                    logging.debug(f"Insert query|{sql_cmd.replace(NL, ' ')}")
                    csr.execute(sql_cmd)
                    self.conn.commit()
                    logging.info(f'Done with {self.src}|{fld}|{rating}|{tctype}')

    def game(self):
        csr = self.conn.cursor()

        sql_cmd = self.delete_stats()
        csr.execute(sql_cmd)
        self.conn.commit()

        sql_cmd = self.delete_cov()
        csr.execute(sql_cmd)
        self.conn.commit()

        for fld in self.fld:
            fldid = q.get_fldid(self.conn, fld)
            for rating in self.rating:
                for tctype in self.timecontrol:
                    tcid = q.get_tcid(self.conn, tctype)
                    for color in self.color:
                        colorid = q.get_colorid(self.conn, color)
                        ct, av, sd, mn, lower, qt1, qt2, qt3, upper, mx = self.aggregate_game(fld, tcid, rating, colorid)
                        sql_cmd = 'INSERT INTO stat.StatisticsSummary (SourceID, AggregationID, MeasurementID, RatingID, TimeControlID, ColorID, EvaluationGroupID, '
                        sql_cmd = sql_cmd + 'RecordCount, Average, StandardDeviation, MinValue, LowerPcnt, LowerQuartile, Median, '
                        sql_cmd = sql_cmd + 'UpperQuartile, UpperPcnt, MaxValue) '
                        sql_cmd = sql_cmd + f"VALUES ({self.src}, {self.agg}, {fldid}, {rating}, {tcid}, {colorid}, 0, "
                        sql_cmd = sql_cmd + f"{ct}, {av}, {sd}, {mn}, {lower}, {qt1}, {qt2}, "
                        sql_cmd = sql_cmd + f"{qt3}, {upper}, {mx})"
                        logging.debug(f"Insert query|{sql_cmd.replace(NL, ' ')}")
                        csr.execute(sql_cmd)
                        self.conn.commit()
                        logging.info(f'Done with {self.src}|{fld}|{rating}|{tctype}|{color}')
