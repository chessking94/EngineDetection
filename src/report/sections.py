import datetime as dt
import logging
import math

import pandas as pd

import outliers
import queries as qry

NL = '\n'
EV_LEN = 35
HDR_LEN = 30
PK_LEN = 10


class report:
    def __init__(self, rpt, compare_stats, typ, conn, eventid, playerid, startdate, enddate):
        self.rpt = rpt
        self.comp_srcid = compare_stats.get('srcId')
        self.comp_tcid = compare_stats.get('tcId')
        self.comp_rid = compare_stats.get('rId')
        self.scoreequal = compare_stats.get('scoreEqual')
        self.score_key = 'ScoreEqual' if compare_stats.get('scoreEqual') else 'Score'
        self.typ = typ
        self.conn = conn
        self.eventid = eventid
        self.playerid = playerid
        self.startdate = startdate
        self.enddate = enddate

    def key_stats(self):
        if self.typ == 'Event':
            self.rpt.write('WHOLE-EVENT STATISTICS:' + NL)
        else:
            self.rpt.write('WHOLE-SAMPLE STATISTICS:' + NL)

        self.rpt.write('-'*25 + NL)

        if self.typ == 'Event':
            self.rpt.write('Average rating by game:'.ljust(EV_LEN, ' '))
            qry_text = qry.event_avgrating(self.eventid)
        else:
            self.rpt.write('Average opponent rating:'.ljust(EV_LEN, ' '))
            qry_text = qry.player_avgrating(self.playerid, self.startdate, self.enddate)
        rs = pd.read_sql(qry_text, self.conn).values.tolist()

        if self.comp_rid:
            rt = self.comp_rid
        else:
            if self.typ == 'Event':
                rt = math.floor(int(rs[0][0])/100)*100
            else:
                rt = math.floor(int(rs[0][3])/100)*100

        self.rpt.write(str(int(rs[0][0])) + '; ')
        self.rpt.write('min ' + str(int(rs[0][1])) + ', ')
        self.rpt.write('max ' + str(int(rs[0][2])) + NL)

        if self.typ == 'Event':
            qry_text = qry.event_totalmoves(self.eventid)
        else:
            qry_text = qry.player_totalmoves(self.playerid, self.startdate, self.enddate)
        totrs = pd.read_sql(qry_text, self.conn)
        totrs = totrs.set_index('TraceKey')

        if self.typ == 'Event':
            qry_text = qry.event_scoredmoves(self.eventid)
        else:
            qry_text = qry.player_scoredmoves(self.playerid, self.startdate, self.enddate)
        rs = pd.read_sql(qry_text, self.conn).values.tolist()

        self.rpt.write('Scored Moves:'.ljust(EV_LEN, ' '))
        self.rpt.write(f"{int(rs[0][1])} / {totrs.loc['Total', 'MoveCount']} = {'{:.2f}'.format(100*int(rs[0][1])/totrs.loc['Total', 'MoveCount'])}%" + NL)
        for key, trace in totrs.iterrows():
            if key != 'Total':
                self.rpt.write(f"{trace['TraceDescription']}:".ljust(EV_LEN, ' '))
                self.rpt.write(f"{trace['MoveCount']} / {totrs.loc['Total', 'MoveCount']} = {'{:.2f}'.format(100*trace['MoveCount']/totrs.loc['Total', 'MoveCount'])}%" + NL)
        self.rpt.write(NL)

        for i in range(5):
            self.rpt.write(f'T{i+1}:'.ljust(EV_LEN, ' '))
            self.rpt.write(f"{int(rs[0][i+2])} / {int(rs[0][1])} = {'{:.2f}'.format(100*int(rs[0][i+2])/int(rs[0][1]))}%" + NL)
        self.rpt.write('Blunders:'.ljust(EV_LEN, ' '))
        self.rpt.write(f"{int(rs[0][9])} / {int(rs[0][1])} = {'{:.2f}'.format(100*int(rs[0][9])/int(rs[0][1]))}%" + NL)
        self.rpt.write('ScACPL:'.ljust(EV_LEN, ' '))
        acpl = outliers.format_cpl('Event', 'ScACPL', rt, rs[0][7], self.conn)
        self.rpt.write(acpl + NL)
        self.rpt.write('ScSDCPL:'.ljust(EV_LEN, ' '))
        sdcpl = outliers.format_cpl('Event', 'ScSDCPL', rt, rs[0][8], self.conn)
        self.rpt.write(sdcpl + NL)

        if self.typ == 'Event':
            qry_text = qry.event_totalscore(self.eventid)
        else:
            qry_text = qry.player_totalscore(self.playerid, self.startdate, self.enddate)
        rss = pd.read_sql(qry_text, self.conn).values.tolist()[0]
        if self.scoreequal:
            score_val = rss[1]
        else:
            score_val = rss[0]

        self.rpt.write('Score:'.ljust(EV_LEN, ' '))
        if score_val >= 99.995:
            score = '100.0*'
        else:
            score = '{:.2f}'.format(score_val)
        self.rpt.write(score + NL)

        if self.comp_tcid not in [5, 6]:
            key_tcid = 5
        else:
            key_tcid = self.comp_tcid

        # hard-coding sourceID since Lichess doesn't have event stats
        z_qry = qry.zscore_data(agg='Event', srcid=3, tcid=key_tcid, rating=rt)
        z_rs = pd.read_sql(z_qry, self.conn)
        z_rs = z_rs.set_index('MeasurementName')

        t1_z = ((int(rs[0][2])/int(rs[0][1])) - z_rs.loc['T1', 'Average'])/z_rs.loc['T1', 'StandardDeviation']
        scacpl_z = -1*(rs[0][7] - z_rs.loc['ScACPL', 'Average'])/z_rs.loc['ScACPL', 'StandardDeviation']
        score_z = (score_val - z_rs.loc[self.score_key, 'Average'])/z_rs.loc[self.score_key, 'StandardDeviation']
        roi = outliers.calc_comp_roi([t1_z, scacpl_z, score_z])
        self.rpt.write('ROI:'.ljust(EV_LEN, ' '))
        self.rpt.write(roi + NL)

        test_arr = [int(rs[0][2])/int(rs[0][1]), rs[0][7], score_val]
        # hard-coding sourceID since Lichess doesn't have event stats
        pval = outliers.get_mah_pval(conn=self.conn, test_arr=test_arr, srcid=3, agg='Event', rating=rt, tcid=key_tcid)
        self.rpt.write('PValue:'.ljust(EV_LEN, ' '))
        self.rpt.write(pval + NL)

        self.rpt.write(NL)
        self.rpt.write(NL)

    def player_summary(self):
        player_len = 30
        elo_len = 7
        rec_len = 13
        perf_len = 7
        evm_len = 24
        blun_len = 24
        acpl_len = 11
        sdcpl_len = 11
        score_len = 10
        roi_len = 9
        pval_len = 11

        self.rpt.write('Player Name'.ljust(player_len, ' '))
        self.rpt.write('Elo'.ljust(elo_len, ' '))
        self.rpt.write('Record'.ljust(rec_len, ' '))
        self.rpt.write('Perf'.ljust(perf_len, ' '))
        self.rpt.write('EVM / Turns = Pcnt'.ljust(evm_len, ' '))
        self.rpt.write('Blund / Turns = Pcnt'.ljust(blun_len, ' '))
        self.rpt.write('ScACPL'.ljust(acpl_len, ' '))
        self.rpt.write('ScSDCPL'.ljust(sdcpl_len, ' '))
        self.rpt.write('Score'.ljust(score_len, ' '))
        self.rpt.write('ROI'.ljust(roi_len, ' '))
        self.rpt.write('PValue'.ljust(pval_len, ' '))
        self.rpt.write('Opp EVM Pcnt'.ljust(evm_len, ' '))
        self.rpt.write('Opp Blund Pcnt'.ljust(blun_len, ' '))
        self.rpt.write('OppScACPL'.ljust(acpl_len, ' '))
        self.rpt.write('OppScSDCPL'.ljust(sdcpl_len, ' '))
        self.rpt.write('OppScore'.ljust(score_len, ' '))
        self.rpt.write('OppROI'.ljust(roi_len, ' '))
        self.rpt.write('OppPValue'.ljust(roi_len, ' '))
        self.rpt.write(NL)
        self.rpt.write('-'*257)
        self.rpt.write(NL)

        if self.eventid:
            qry_text = qry.event_playersummary(self.eventid)
        else:
            qry_text = qry.player_playersummary(self.playerid, self.startdate, self.enddate)
        rs = pd.read_sql(qry_text, self.conn)
        for idx, player in rs.iterrows():
            self.rpt.write(player['Name'][0:player_len].ljust(player_len, ' '))
            if self.comp_rid:
                rt = self.comp_rid
            else:
                rt = int(math.floor(player['Rating']/100)*100)
            self.rpt.write(str(player['Rating']).ljust(elo_len, ' '))

            if player['GamesPlayed'] < 10:
                gp_len = 3
            elif player['GamesPlayed'] >= 100:
                gp_len = 5
            else:
                gp_len = 4
            rec = str(player['Record']).ljust(gp_len, ' ') + ' / ' + str(player['GamesPlayed'])
            self.rpt.write(rec.ljust(rec_len, ' '))

            perf = str(player['Perf'])
            perf = '+' + perf if perf[0] != '-' else perf
            self.rpt.write(perf.ljust(perf_len, ' '))

            agg_typ = 'Event'
            evm = str(player['EVM']).ljust(4, ' ') + ' / ' + str(player['ScoredMoves']).ljust(4, ' ') + ' = '
            evmpcnt = outliers.format_evm(agg_typ, rt, 100*player['EVM']/player['ScoredMoves'], 1, self.conn)
            evm = evm + evmpcnt
            self.rpt.write(evm.ljust(evm_len, ' '))

            bl = str(player['Blunders']).ljust(4, ' ') + ' / ' + str(player['ScoredMoves']).ljust(4, ' ') + ' = '
            blpcnt = '{:3.2f}'.format(100*player['Blunders']/player['ScoredMoves']) + '%'
            bl = bl + blpcnt
            self.rpt.write(bl.ljust(blun_len, ' '))

            acpl = outliers.format_cpl(agg_typ, 'ScACPL', rt, player['ACPL'], self.conn)
            self.rpt.write(acpl.ljust(acpl_len, ' '))

            sdcpl = outliers.format_cpl(agg_typ, 'ScSDCPL', rt, player['SDCPL'], self.conn)
            self.rpt.write(sdcpl.ljust(sdcpl_len, ' '))

            if player[self.score_key] >= 99.995:
                score = '100.0*'
            else:
                score = '{:.2f}'.format(player[self.score_key])
            self.rpt.write(score.ljust(score_len, ' '))

            if self.comp_tcid not in [5, 6]:
                sum_tcid = 5
            else:
                sum_tcid = self.comp_tcid

            # hard-coding sourceID since Lichess doesn't have event stats
            z_qry = qry.zscore_data(agg=agg_typ, srcid=3, tcid=sum_tcid, rating=rt)
            z_rs = pd.read_sql(z_qry, self.conn)
            z_rs = z_rs.set_index('MeasurementName')

            t1_z = ((player['EVM']/player['ScoredMoves']) - z_rs.loc['T1', 'Average'])/z_rs.loc['T1', 'StandardDeviation']
            scacpl_z = -1*(player['ACPL'] - z_rs.loc['ScACPL', 'Average'])/z_rs.loc['ScACPL', 'StandardDeviation']
            score_z = (player[self.score_key] - z_rs.loc[self.score_key, 'Average'])/z_rs.loc[self.score_key, 'StandardDeviation']
            roi = outliers.calc_comp_roi([t1_z, scacpl_z, score_z])
            self.rpt.write(roi.ljust(roi_len, ' '))

            test_arr = [player['EVM']/player['ScoredMoves'], player['ACPL'], player[self.score_key]]
            # hard-coding sourceID since Lichess doesn't have event stats
            pval = outliers.get_mah_pval(conn=self.conn, test_arr=test_arr, srcid=3, agg=agg_typ, rating=rt, tcid=sum_tcid)
            self.rpt.write(pval.ljust(pval_len, ' '))

            oppevm = str(player['OppEVM']).ljust(4, ' ') + ' / ' + str(player['OppScoredMoves']).ljust(4, ' ') + ' = '
            oppevm = oppevm + '{:3.1f}'.format(100*player['OppEVM']/player['OppScoredMoves']) + '%'
            self.rpt.write(oppevm.ljust(evm_len, ' '))

            oppbl = str(player['OppBlunders']).ljust(4, ' ') + ' / ' + str(player['OppScoredMoves']).ljust(4, ' ') + ' = '
            oppblpcnt = '{:3.2f}'.format(100*player['OppBlunders']/player['OppScoredMoves']) + '%'
            oppbl = oppbl + oppblpcnt
            self.rpt.write(oppbl.ljust(blun_len, ' '))

            oppacpl = '{:.4f}'.format(player['OppACPL'])
            self.rpt.write(oppacpl.ljust(acpl_len, ' '))

            oppsdcpl = '{:.4f}'.format(player['OppSDCPL'])
            self.rpt.write(oppsdcpl.ljust(sdcpl_len, ' '))

            if player[f'Opp{self.score_key}'] >= 99.995:
                oppscore = '100.0*'
            else:
                oppscore = '{:.2f}'.format(player[f'Opp{self.score_key}'])
            self.rpt.write(oppscore.ljust(score_len, ' '))

            opp_t1_z = ((player['OppEVM']/player['OppScoredMoves']) - z_rs.loc['T1', 'Average'])/z_rs.loc['T1', 'StandardDeviation']
            opp_scacpl_z = -1*(player['OppACPL'] - z_rs.loc['ScACPL', 'Average'])/z_rs.loc['ScACPL', 'StandardDeviation']
            opp_score_z = (player[f'Opp{self.score_key}'] - z_rs.loc[self.score_key, 'Average'])/z_rs.loc[self.score_key, 'StandardDeviation']
            opproi = outliers.calc_comp_roi([opp_t1_z, opp_scacpl_z, opp_score_z])
            self.rpt.write(opproi.ljust(roi_len, ' '))

            # opp p-value
            opp_test_arr = [player['OppEVM']/player['OppScoredMoves'], player['OppACPL'], player[f'Opp{self.score_key}']]
            opppval = outliers.get_mah_pval(conn=self.conn, test_arr=opp_test_arr, srcid=3, agg=agg_typ, rating=rt, tcid=sum_tcid)
            self.rpt.write(opppval.ljust(pval_len, ' '))

            self.rpt.write(NL)

        self.rpt.write(NL)
        self.rpt.write(NL)

    def game_traces(self):
        self.rpt.write('-'*31)
        self.rpt.write(NL)

        if self.eventid:
            qry_text = qry.event_playergames(self.eventid)
        else:
            qry_text = qry.player_playergames(self.playerid, self.startdate, self.enddate)
        player_rs = pd.read_sql(qry_text, self.conn)
        for i, player in player_rs.iterrows():
            self.rpt.write(player['Name'])
            if self.comp_rid:
                rt = self.comp_rid
            else:
                rt = int(math.floor(player['Rating']/100)*100)
            self.rpt.write(' ' + str(player['Rating']))
            self.rpt.write(f" (Moves={player['ScoredMoves']})")
            self.rpt.write(NL)

            if self.eventid:
                qry_text = qry.event_playeropp(player['PlayerID'], self.eventid)
            else:
                qry_text = qry.player_playeropp(player['PlayerID'], self.startdate, self.enddate)
            game_rs = pd.read_sql(qry_text, self.conn)
            for ii, game in game_rs.iterrows():
                g_id = int(game['GameID'])
                rd = str(int(game['RoundNum'])) if game['RoundNum'] else ''
                if len(rd) == 1:
                    self.rpt.write(' ')
                    self.rpt.write(rd)
                elif len(rd) > 2:
                    self.rpt.write('  ')
                else:
                    self.rpt.write(rd)

                self.rpt.write(game['Color'] + ' ')
                self.rpt.write(game['Result'] + ' ')
                self.rpt.write(game['OppName'][0:25].ljust(25, ' '))
                self.rpt.write(str(game['OppRating']).ljust(4, ' ') + ':  ')

                agg_typ = 'Game'
                evm = str(game['EVM']).ljust(3, ' ') + ' / ' + str(game['ScoredMoves']).ljust(3, ' ') + ' = '
                evmpcnt = outliers.format_evm(agg_typ, rt, 100*game['EVM']/game['ScoredMoves'], 0, self.conn)
                evm = evm + evmpcnt
                self.rpt.write(evm.ljust(18, ' '))

                c = 1 if game['Color'] == 'w' else 2
                acpl = outliers.format_cpl(agg_typ, 'ScACPL', rt, game['ACPL'], self.conn, c)
                self.rpt.write(acpl.ljust(8, ' '))

                sdcpl = outliers.format_cpl(agg_typ, 'ScSDCPL', rt, game['SDCPL'], self.conn, c)
                self.rpt.write(sdcpl.ljust(8, ' '))

                if game[self.score_key] >= 99.995:
                    score = '100.0*'
                else:
                    score = '{:.2f}'.format(game[self.score_key])
                self.rpt.write(score.ljust(7, ' '))

                z_qry = qry.zscore_data(agg=agg_typ, srcid=self.comp_srcid, tcid=self.comp_tcid, rating=rt, colorid=c)
                z_rs = pd.read_sql(z_qry, self.conn)
                z_rs = z_rs.set_index('MeasurementName')

                t1_z = ((game['EVM']/game['ScoredMoves']) - z_rs.loc['T1', 'Average'])/z_rs.loc['T1', 'StandardDeviation']
                scacpl_z = -1*(game['ACPL'] - z_rs.loc['ScACPL', 'Average'])/z_rs.loc['ScACPL', 'StandardDeviation']
                score_z = (game[self.score_key] - z_rs.loc[self.score_key, 'Average'])/z_rs.loc[self.score_key, 'StandardDeviation']

                roi = outliers.calc_comp_roi([t1_z, scacpl_z, score_z])
                self.rpt.write(roi.ljust(6, ' '))

                test_arr = [game['EVM']/game['ScoredMoves'], game['ACPL'], game[self.score_key]]
                pval = outliers.get_mah_pval(conn=self.conn, test_arr=test_arr, srcid=self.comp_srcid, agg=agg_typ, rating=rt, tcid=self.comp_tcid, colorid=c)
                self.rpt.write(pval.ljust(8, ' '))

                # moves
                qry_text = qry.game_trace(g_id, c)
                moves_rs = pd.read_sql(qry_text, self.conn)
                ctr = 0
                for iii, mv in moves_rs.iterrows():
                    if ctr == 60:
                        self.rpt.write(NL)
                        self.rpt.write(' '*93)
                        ctr = 0
                    elif ctr % 10 == 0 and ctr > 0:
                        self.rpt.write(' ')
                    self.rpt.write(mv['MoveTrace'])
                    ctr = ctr + 1

                self.rpt.write(NL)

            self.rpt.write('-'*25)
            self.rpt.write(NL)


class general:
    def __init__(self, rpt, compare_stats):
        self.rpt = rpt
        self.srcname = compare_stats.get('srcName')
        self.tcname = compare_stats.get('tcName')
        self.rating = compare_stats.get('rId')
        self.scoreequal = compare_stats.get('scoreEqual')

    def header_type(self, rpt_typ, conn, event, name, startdate, enddate):
        self.rpt.write('-'*100 + NL)
        self.rpt.write('Analysis Type:'.ljust(HDR_LEN, ' ') + rpt_typ + NL)
        self.rpt.write('Compared Source:'.ljust(HDR_LEN, ' ') + self.srcname + NL)
        self.rpt.write('Compared Time Control:'.ljust(HDR_LEN, ' ') + self.tcname + NL)
        self.rpt.write('Compared Rating:'.ljust(HDR_LEN, ' '))
        if self.rating:
            self.rpt.write(str(self.rating) + NL)
        else:
            self.rpt.write('Current values' + NL)
        self.rpt.write('Score Values Relative To:'.ljust(HDR_LEN, ' '))
        if self.scoreequal:
            self.rpt.write('Classical OTB' + NL)
        else:
            self.rpt.write('Same Source and Time Control' + NL)
        self.rpt.write(NL)

        if rpt_typ == 'Event':
            qry_text = qry.event_summary(event)
            rs = pd.read_sql(qry_text, conn).values.tolist()
            self.rpt.write('Event Name:'.ljust(HDR_LEN, ' ') + event + NL)
            self.rpt.write('Event Date:'.ljust(HDR_LEN, ' ') + rs[0][1] + NL)
            rd = str(int(rs[0][2])) if rs[0][2] else ''
            self.rpt.write('Rounds:'.ljust(HDR_LEN, ' ') + rd + NL)
            self.rpt.write('Players:'.ljust(HDR_LEN, ' ') + str(int(rs[0][3])) + NL)
        elif rpt_typ == 'Player':
            self.rpt.write('Player Name:'.ljust(HDR_LEN, ' ') + ' '.join(name) + NL)
            self.rpt.write('Games Between:'.ljust(HDR_LEN, ' ') + startdate + ' - ' + enddate + NL)
        self.rpt.write(NL)

    def header_info(self, engine, depth):
        dte = dt.datetime.now().strftime('%m/%d/%Y')
        self.rpt.write('Engine Name:'.ljust(HDR_LEN, ' ') + engine + NL)
        self.rpt.write('Depth:'.ljust(HDR_LEN, ' ') + str(depth) + NL)
        self.rpt.write('Report Date:'.ljust(HDR_LEN, ' ') + dte + NL)
        self.rpt.write(NL)
        self.rpt.write(NL)

    def scoring_desc(self, conn):
        qry_text = qry.max_eval()
        mx_ev = pd.read_sql(qry_text, conn).values.tolist()
        mx_ev = str(int(mx_ev[0][0]))
        self.rpt.write('MOVE SCORING' + NL)
        self.rpt.write('-'*25 + NL)
        self.rpt.write('A move is Scored if it does not meet any of the following:' + NL)
        self.rpt.write('Is theoretical opening move' + NL)
        self.rpt.write('Is a tablebase hit' + NL)
        self.rpt.write(f'The best engine evaluation is greater than {mx_ev} centipawns or a mate in N' + NL)
        self.rpt.write('The engine evaluation of the move played is a mate in N' + NL)
        self.rpt.write('Only one legal move exists or the difference in evaluation between the top 2 engine moves is greater than 200 centipawns' + NL)
        self.rpt.write('Is the second or third occurance of the position' + NL)
        self.rpt.write(NL)
        self.rpt.write(NL)

    def player_key(self):
        self.rpt.write('PLAYER KEY' + NL)
        self.rpt.write('-'*100 + NL)
        self.rpt.write('EVM:'.ljust(PK_LEN, ' '))
        self.rpt.write('Equal Value Match; moves with an evaluation that matches the best engine evaluation' + NL)
        self.rpt.write('Blund:'.ljust(PK_LEN, ' '))
        self.rpt.write('Blunders; moves that lost 200 centipawns or more' + NL)
        self.rpt.write('ScACPL:'.ljust(PK_LEN, ' '))
        self.rpt.write('Scaled Average Centipawn Loss; sum of total centipawn loss divided by the number of moves, scaled by position evaluation' + NL)
        self.rpt.write('ScSDCPL:'.ljust(PK_LEN, ' '))
        self.rpt.write('Scaled Standard Deviation Centipawn Loss; standard deviation of centipawn loss values from each move played, scaled by position evaluation' + NL)
        self.rpt.write('Score:'.ljust(PK_LEN, ' '))
        self.rpt.write('Game Score; measurement of how accurately the game was played, ranges from 0 to 100' + NL)
        self.rpt.write('ROI:'.ljust(PK_LEN, ' '))
        self.rpt.write('Raw Outlier Index; standardized value where 50 represents the mean for that rating level and each increment of 5 is one standard deviation' + NL)
        self.rpt.write('PValue:'.ljust(PK_LEN, ' '))
        self.rpt.write('Chi-square statistic associated with the Mahalanobis distance of the test point (T1, ScACPL, Score)' + NL)
        self.rpt.write(' '*PK_LEN + 'An asterisk (*) following any statistic indicates an outlier that should be reviewed more closely' + NL)
        self.rpt.write(NL)

    def game_key(self):
        self.rpt.write('GAME KEY' + NL)
        self.rpt.write('-'*100 + NL)
        self.rpt.write('(Player Name) (Elo) (Scored Moves)' + NL)
        self.rpt.write(' (Round)(Color) (Result) (Opp) (Opp Rating): (EVM/Turns = EVM%) (ScACPL) (ScSDCPL) (Score) (ROI) (PValue) (game trace)' + NL)
        self.rpt.write('Game trace key: b = Book move; M = EV match; 0 = Inferior move; e = Eliminated because one side far ahead; ')
        self.rpt.write('t = Tablebase hit; f = Forced move; r = Repeated move' + NL)
        self.rpt.write(NL)


def update_maxeval(conn, maxeval):
    go = False
    try:
        float(maxeval)
        go = True
    except ValueError:
        logging.warning(f'Non-numeric max eval|{maxeval}')

    if go:
        csr = conn.cursor()
        sql_cmd = f"UPDATE Settings SET Value = '{maxeval}' WHERE ID = 3"
        logging.debug(f"Update query|{sql_cmd}")
        csr.execute(sql_cmd)
        conn.commit()
        rtn = maxeval
    else:
        qry_text = 'SELECT Value FROM Settings WHERE ID = 3'
        rs = pd.read_sql(qry_text, conn).values.tolist()
        rtn = rs[0][0]

    return rtn
