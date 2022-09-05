import logging

import numpy as np
import pandas as pd

import Queries as q


class aggregator:
    def __init__(self, conn, agg, src, fld, timecontrol, rating, evalgroup, color):
        self.conn = conn
        self.agg = agg
        self.src = src
        self.fld = fld
        self.timecontrol = timecontrol
        self.rating = rating
        self.evalgroup = evalgroup
        self.color = color

    def aggregate_evals(self, fld, tctype, rating, evalgroup, color):
        if fld == 'ACPL':
            qry_text = q.eval_acpl(self.src, tctype, rating, evalgroup, color)
        else:
            N = fld[1:2]
            qry_text = q.eval_tx(self.src, tctype, rating, evalgroup, color, N)
        data_np = pd.read_sql(qry_text, self.conn).to_numpy()
        if len(data_np) > 0:
            data_arr = np.concatenate(data_np)

            ct = len(data_arr)
            av = np.mean(data_arr)
            sd = np.std(data_arr)

            ci_min = 1
            ci_max = 100 - ci_min
            lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt = np.percentile(data_arr, [ci_min, 25, 50, 75, ci_max])
        else:
            ct = 0
            av = 'NULL'
            sd = 'NULL'
            lower_pcnt = 'NULL'
            qtr1 = 'NULL'
            qtr2 = 'NULL'
            qtr3 = 'NULL'
            upper_pcnt = 'NULL'

        return [ct, av, sd, lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt]

    def aggregate_event(self, fld, tctype, rating):
        if fld == 'ACPL':
            qry_text = q.event_acpl(tctype, rating)
        elif fld == 'SDCPL':
            qry_text = q.event_sdcpl(tctype, rating)
        elif fld == 'Score':
            qry_text = q.event_score(tctype, rating)
        else:
            N = fld[1:2]
            qry_text = q.event_tx(tctype, rating, N)
        data_np = pd.read_sql(qry_text, self.conn).to_numpy()
        if len(data_np) > 0:
            data_arr = np.concatenate(data_np)

            ct = len(data_arr)
            av = np.mean(data_arr)
            sd = np.std(data_arr)

            ci_min = 1
            ci_max = 100 - ci_min
            lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt = np.percentile(data_arr, [ci_min, 25, 50, 75, ci_max])
        else:
            ct = 0
            av = 'NULL'
            sd = 'NULL'
            lower_pcnt = 'NULL'
            qtr1 = 'NULL'
            qtr2 = 'NULL'
            qtr3 = 'NULL'
            upper_pcnt = 'NULL'

        return [ct, av, sd, lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt]

    def aggregate_game(self, fld, tctype, rating, color):
        if fld == 'ACPL':
            qry_text = q.game_acpl(self.src, tctype, rating, color)
        elif fld == 'SDCPL':
            qry_text = q.game_sdcpl(self.src, tctype, rating, color)
        elif fld == 'Score':
            qry_text = q.game_score(self.src, tctype, rating, color)
        else:
            N = fld[1:2]
            qry_text = q.game_tx(self.src, tctype, rating, color, N)
        data_np = pd.read_sql(qry_text, self.conn).to_numpy()
        if len(data_np) > 0:
            data_arr = np.concatenate(data_np)

            ct = len(data_arr)
            av = np.mean(data_arr)
            sd = np.std(data_arr)

            ci_min = 1
            ci_max = 100 - ci_min
            lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt = np.percentile(data_arr, [ci_min, 25, 50, 75, ci_max])

            if fld == 'Score' and qtr3 > 100:
                qtr3 = 100
            if fld == 'Score' and upper_pcnt > 100:
                upper_pcnt = 100
        else:
            ct = 0
            av = 'NULL'
            sd = 'NULL'
            lower_pcnt = 'NULL'
            qtr1 = 'NULL'
            qtr2 = 'NULL'
            qtr3 = 'NULL'
            upper_pcnt = 'NULL'

        return [ct, av, sd, lower_pcnt, qtr1, qtr2, qtr3, upper_pcnt]

    def evaluation(self):
        csr = self.conn.cursor()

        sql_cmd = f"DELETE FROM StatisticsSummary WHERE Source = '{self.src}' AND Aggregation = '{self.agg}'"
        csr.execute(sql_cmd)
        self.conn.commit()

        for fld in self.fld:
            for rating in self.rating:
                for tctype in self.timecontrol:
                    for color in self.color:
                        for evalgroup in self.evalgroup:
                            ct, av, sd, lower, qt1, qt2, qt3, upper = self.aggregate_evals(fld, tctype, rating, evalgroup, color)
                            sql_cmd = 'INSERT INTO StatisticsSummary (Source, Aggregation, Field, Rating, TimeControlType, Color, '
                            sql_cmd = sql_cmd + 'EvalGroup, Count, Average, StandardDeviation, LowerPcnt, LowerQuartile, Median, '
                            sql_cmd = sql_cmd + 'UpperQuartile, UpperPcnt) '
                            sql_cmd = sql_cmd + f"VALUES ('{self.src}', '{self.agg}', '{fld}', {rating}, '{tctype}', '{color}', "
                            sql_cmd = sql_cmd + f"{evalgroup}, {ct}, {av}, {sd}, {lower}, {qt1}, {qt2}, "
                            sql_cmd = sql_cmd + f"{qt3}, {upper})"
                            csr.execute(sql_cmd)
                            self.conn.commit()
                            logging.info(f'Done with {fld}|{rating}|{evalgroup}|{color}|{tctype}')

    def event(self):
        csr = self.conn.cursor()

        sql_cmd = f"DELETE FROM StatisticsSummary WHERE Source = '{self.src}' AND Aggregation = '{self.agg}'"
        csr.execute(sql_cmd)
        self.conn.commit()

        for fld in self.fld:
            for rating in self.rating:
                for tctype in self.timecontrol:
                    ct, av, sd, lower, qt1, qt2, qt3, upper = self.aggregate_event(fld, tctype, rating)
                    sql_cmd = 'INSERT INTO StatisticsSummary (Source, Aggregation, Field, Rating, TimeControlType, Color, EvalGroup, '
                    sql_cmd = sql_cmd + 'Count, Average, StandardDeviation, LowerPcnt, LowerQuartile, Median, UpperQuartile, UpperPcnt) '
                    sql_cmd = sql_cmd + f"VALUES ('{self.src}', '{self.agg}', '{fld}', {rating}, '{tctype}', 'N/A', 0, "
                    sql_cmd = sql_cmd + f"{ct}, {av}, {sd}, {lower}, {qt1}, {qt2}, {qt3}, {upper})"
                    csr.execute(sql_cmd)
                    self.conn.commit()
                    logging.info(f'Done with Field = {fld}, Rating = {rating}, TimeControlType = {tctype}')

    def game(self):
        csr = self.conn.cursor()

        sql_cmd = f"DELETE FROM StatisticsSummary WHERE Source = '{self.src}' AND Aggregation = '{self.agg}'"
        csr.execute(sql_cmd)
        self.conn.commit()

        for fld in self.fld:
            for rating in self.rating:
                for tctype in self.timecontrol:
                    for color in self.color:
                        ct, av, sd, lower, qt1, qt2, qt3, upper = self.aggregate_game(fld, tctype, rating, color)
                        sql_cmd = 'INSERT INTO StatisticsSummary (Source, Aggregation, Field, Rating, TimeControlType, Color, EvalGroup, '
                        sql_cmd = sql_cmd + 'Count, Average, StandardDeviation, LowerPcnt, LowerQuartile, Median, '
                        sql_cmd = sql_cmd + 'UpperQuartile, UpperPcnt) '
                        sql_cmd = sql_cmd + f"VALUES ('{self.src}', '{self.agg}', '{fld}', {rating}, '{tctype}', '{color}', 0, "
                        sql_cmd = sql_cmd + f"{ct}, {av}, {sd}, {lower}, {qt1}, {qt2}, "
                        sql_cmd = sql_cmd + f"{qt3}, {upper})"
                        csr.execute(sql_cmd)
                        self.conn.commit()
                        logging.info(f'Done with Field = {fld}, Rating = {rating}, TimeControlType = {tctype}, Color = {color}')
