import datetime as dt
import math

import pandas as pd

import outliers
import queries as qry

NL = '\n'
EV_LEN = 35
HDR_LEN = 20
PK_LEN = 10


class report:
    def __init__(self, rpt, typ, conn, event, name, startdate, enddate):
        self.rpt = rpt
        self.typ = typ
        self.conn = conn
        self.event = event
        self.name = name
        self.startdate = startdate
        self.enddate = enddate

    def key_stats(self):
        if self.typ == 'Event':
            self.rpt.write('Whole-event statistics:' + NL)
        else:
            self.rpt.write('Whole-sample statistics:' + NL)

        self.rpt.write('-'*25 + NL)

        if self.typ == 'Event':
            self.rpt.write('Average rating by game:'.ljust(EV_LEN, ' '))
            qry_text = qry.event_avgrating(self.event)
        else:
            self.rpt.write('Average opponent rating:'.ljust(EV_LEN, ' '))
            qry_text = qry.player_avgrating(self.name, self.startdate, self.enddate)
        rs = pd.read_sql(qry_text, self.conn).values.tolist()

        if self.typ == 'Event':
            rt = math.floor(int(rs[0][0])/100)*100
        else:
            rt = math.floor(int(rs[0][3])/100)*100
        self.rpt.write(str(int(rs[0][0])) + '; ')
        self.rpt.write('min ' + str(int(rs[0][1])) + ', ')
        self.rpt.write('max ' + str(int(rs[0][2])) + NL)

        if self.typ == 'Event':
            qry_text = qry.event_totalmoves(self.event)
        else:
            qry_text = qry.player_totalmoves(self.name, self.startdate, self.enddate)
        rs = pd.read_sql(qry_text, self.conn).values.tolist()
        mvs = int(rs[0][0])

        if self.typ == 'Event':
            qry_text = qry.event_scoredmoves(self.event)
        else:
            qry_text = qry.player_scoredmoves(self.name, self.startdate, self.enddate)
        rs = pd.read_sql(qry_text, self.conn).values.tolist()

        if self.typ == 'Event':
            self.rpt.write('Average rating for scored moves:'.ljust(EV_LEN, ' ') + str(int(rs[0][0])) + NL)
        self.rpt.write('Scored Moves percentage:'.ljust(EV_LEN, ' '))
        self.rpt.write(str(int(rs[0][1])) + ' / ' + str(mvs) + ' = ' + '{:.2f}'.format(100*int(rs[0][1])/mvs) + '%' + NL)
        self.rpt.write(NL)

        qry_text = qry.max_eval()
        mx_ev = pd.read_sql(qry_text, self.conn).values.tolist()
        mx_ev = str(int(mx_ev[0][0]))
        self.rpt.write(f'A move is Scored if it does not meet any of the following: Theoretical move, tablebase move, or one side is up by {mx_ev} centipawns' + NL)
        self.rpt.write(NL)

        self.rpt.write('Total T1:'.ljust(EV_LEN, ' '))
        self.rpt.write(str(int(rs[0][2])) + ' / ' + str(int(rs[0][1])) + ' = ' + '{:.2f}'.format(100*int(rs[0][2])/int(rs[0][1])) + '%' + NL)
        self.rpt.write('Total T2:'.ljust(EV_LEN, ' '))
        self.rpt.write(str(int(rs[0][3])) + ' / ' + str(int(rs[0][1])) + ' = ' + '{:.2f}'.format(100*int(rs[0][3])/int(rs[0][1])) + '%' + NL)
        self.rpt.write('Total T3:'.ljust(EV_LEN, ' '))
        self.rpt.write(str(int(rs[0][4])) + ' / ' + str(int(rs[0][1])) + ' = ' + '{:.2f}'.format(100*int(rs[0][4])/int(rs[0][1])) + '%' + NL)
        self.rpt.write('Total T4:'.ljust(EV_LEN, ' '))
        self.rpt.write(str(int(rs[0][5])) + ' / ' + str(int(rs[0][1])) + ' = ' + '{:.2f}'.format(100*int(rs[0][5])/int(rs[0][1])) + '%' + NL)
        self.rpt.write('Total T5:'.ljust(EV_LEN, ' '))
        self.rpt.write(str(int(rs[0][6])) + ' / ' + str(int(rs[0][1])) + ' = ' + '{:.2f}'.format(100*int(rs[0][6])/int(rs[0][1])) + '%' + NL)
        self.rpt.write('Total ACPL:'.ljust(EV_LEN, ' '))
        acpl = outliers.format_cpl('Event', 'ACPL', rt, rs[0][7], self.conn)
        self.rpt.write(acpl + NL)
        self.rpt.write('Total SDCPL:'.ljust(EV_LEN, ' '))
        sdcpl = outliers.format_cpl('Event', 'SDCPL', rt, rs[0][8], self.conn)
        self.rpt.write(sdcpl + NL)
        self.rpt.write(NL)

        if self.typ == 'Event':
            qry_text = qry.event_totalscore(self.event)
        else:
            qry_text = qry.player_totalscore(self.name, self.startdate, self.enddate)
        rs = pd.read_sql(qry_text, self.conn).values.tolist()

        if self.typ == 'Event':
            self.rpt.write('Overall event score:'.ljust(EV_LEN, ' '))
        else:
            self.rpt.write('Overall player score:'.ljust(EV_LEN, ' '))
        self.rpt.write('{:2.2f}'.format(rs[0][0]) + NL)

        agg_typ = 'Event'
        z_qry = qry.roi_calc(agg=agg_typ, src='Control', tc='Classical', rating=rt)
        z_rs = pd.read_sql(z_qry, self.conn).values.tolist()
        roi = outliers.calc_roi(agg_typ, rs[0][0], z_rs)

        if self.typ == 'Event':
            self.rpt.write('Overall event ROI:'.ljust(EV_LEN, ' '))
        else:
            self.rpt.write('Overall player ROI:'.ljust(EV_LEN, ' '))
        self.rpt.write(roi + NL)

        self.rpt.write(NL)
        self.rpt.write(NL)

    def player_summary(self):
        player_len = 30
        elo_len = 7
        rec_len = 16
        perf_len = 8
        evm_len = 24
        acpl_len = 10
        sdcpl_len = 10
        score_len = 10
        roi_len = 8

        self.rpt.write('Player Name'.ljust(player_len, ' '))
        self.rpt.write('Elo'.ljust(elo_len, ' '))
        self.rpt.write('Record'.ljust(rec_len, ' '))
        self.rpt.write('Perf'.ljust(perf_len, ' '))
        self.rpt.write('EVM / Turns = Pcnt'.ljust(evm_len, ' '))
        self.rpt.write('ACPL'.ljust(acpl_len, ' '))
        self.rpt.write('SDCPL'.ljust(sdcpl_len, ' '))
        self.rpt.write('Score'.ljust(score_len, ' '))
        self.rpt.write('ROI'.ljust(roi_len, ' '))
        self.rpt.write('Opp EVM / Turns = Pcnt'.ljust(24, ' '))
        self.rpt.write('OppACPL'.ljust(acpl_len, ' '))
        self.rpt.write('OppSDCPL'.ljust(sdcpl_len, ' '))
        self.rpt.write('OppScore'.ljust(score_len, ' '))
        # self.rpt.write('OppROI'.ljust(roi_len, ' '))
        self.rpt.write(NL)
        self.rpt.write('-'*184)
        self.rpt.write(NL)

        if self.event:
            qry_text = qry.event_playersummary(self.event)
        else:
            qry_text = qry.player_playersummary(self.name, self.startdate, self.enddate)
        rs = pd.read_sql(qry_text, self.conn)
        for idx, player in rs.iterrows():
            self.rpt.write(player['Name'][0:player_len].ljust(player_len, ' '))
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
            evm = str(player['EVM']) .ljust(4, ' ') + ' / ' + str(player['ScoredMoves']).ljust(4, ' ') + ' = '
            evmpcnt = outliers.format_evm(agg_typ, rt, 100*player['EVM']/player['ScoredMoves'], 1, self.conn)
            evm = evm + evmpcnt
            self.rpt.write(evm.ljust(evm_len, ' '))

            acpl = outliers.format_cpl(agg_typ, 'ACPL', rt, player['ACPL'], self.conn)
            self.rpt.write(acpl.ljust(acpl_len, ' '))

            sdcpl = outliers.format_cpl(agg_typ, 'SDCPL', rt, player['SDCPL'], self.conn)
            self.rpt.write(sdcpl.ljust(sdcpl_len, ' '))

            score = '{:.2f}'.format(player['Score'])
            self.rpt.write(score.ljust(score_len, ' '))

            z_qry = qry.roi_calc(agg=agg_typ, src='Control', tc='Classical', rating=rt)
            z_rs = pd.read_sql(z_qry, self.conn).values.tolist()
            roi = outliers.calc_roi(agg_typ, player['Score'], z_rs)
            self.rpt.write(roi.ljust(roi_len, ' '))

            oppevm = str(player['OppEVM']) .ljust(4, ' ') + ' / ' + str(player['OppScoredMoves']).ljust(4, ' ') + ' = '
            oppevm = oppevm + '{:3.1f}'.format(100*player['OppEVM']/player['OppScoredMoves']) + '%'
            self.rpt.write(oppevm.ljust(evm_len, ' '))

            oppacpl = '{:.4f}'.format(player['OppACPL'])
            self.rpt.write(oppacpl.ljust(acpl_len, ' '))

            oppsdcpl = '{:.4f}'.format(player['OppSDCPL'])
            self.rpt.write(oppsdcpl.ljust(sdcpl_len, ' '))

            oppscore = '{:.2f}'.format(player['OppScore'])
            self.rpt.write(oppscore.ljust(score_len, ' '))

            # z_qry = qry.roi_calc(agg=agg_typ, src='Control', tc='Classical', rating=rt)
            # z_rs = pd.read_sql(z_qry, self.conn).values.tolist()
            # opproi = outliers.calc_roi(agg_typ, player['OppScore'], z_rs)
            # self.rpt.write(opproi)

            self.rpt.write(NL)

        self.rpt.write(NL)
        self.rpt.write(NL)

    def game_traces(self):
        self.rpt.write('-'*25)
        self.rpt.write(NL)

        if self.event:
            qry_text = qry.event_playergames(self.event)
        else:
            qry_text = qry.player_playergames(self.name, self.startdate, self.enddate)
        player_rs = pd.read_sql(qry_text, self.conn)
        for i, player in player_rs.iterrows():
            self.rpt.write(player['Name'])
            rt = int(math.floor(player['Rating']/100)*100)
            self.rpt.write(' ' + str(player['Rating']))
            self.rpt.write(' ' + str(player['ScoredMoves']))
            self.rpt.write(NL)

            if self.event:
                qry_text = qry.event_playeropp(player['Name'], self.event)
            else:
                qry_text = qry.player_playeropp(player['Name'], self.startdate, self.enddate)
            game_rs = pd.read_sql(qry_text, self.conn)
            for ii, game in game_rs.iterrows():
                g_id = int(game['GameID'])
                rd = str(int(game['RoundNum']))
                if len(rd) == 1:
                    self.rpt.write(' ')
                    self.rpt.write(rd)
                elif len(rd) > 2:
                    self.rpt.write('  ')
                else:
                    self.rpt.write(rd)

                self.rpt.write(game['Color'] + ' ')
                self.rpt.write(game['Result'] + ' ')
                self.rpt.write(game['OppName'][0:21].ljust(20, ' '))
                self.rpt.write(str(game['OppRating']) + ':  ')

                agg_typ = 'Game'
                evm = str(game['EVM']).ljust(3, ' ') + ' / ' + str(game['ScoredMoves']).ljust(3, ' ') + ' = '
                evmpcnt = outliers.format_evm(agg_typ, rt, 100*game['EVM']/game['ScoredMoves'], 0, self.conn)
                evm = evm + evmpcnt
                self.rpt.write(evm.ljust(18, ' '))

                c = 'White' if game['Color'] == 'w' else 'Black'
                acpl = outliers.format_cpl(agg_typ, 'ACPL', rt, game['ACPL'], self.conn, c)
                self.rpt.write(acpl.ljust(8, ' '))

                sdcpl = outliers.format_cpl(agg_typ, 'SDCPL', rt, game['SDCPL'], self.conn, c)
                self.rpt.write(sdcpl.ljust(8, ' '))

                score = '{:.2f}'.format(game['Score'])
                self.rpt.write(score.ljust(7, ' '))

                z_qry = qry.roi_calc(agg=agg_typ, src='Control', tc='Classical', rating=rt, color=c)
                z_rs = pd.read_sql(z_qry, self.conn).values.tolist()
                roi = outliers.calc_roi(agg_typ, game['Score'], z_rs)
                self.rpt.write(roi.ljust(7, ' '))

                # moves
                qry_text = qry.game_trace(g_id, c)
                moves_rs = pd.read_sql(qry_text, self.conn)
                ctr = 0
                for iii, mv in moves_rs.iterrows():
                    self.rpt.write(mv['MoveTrace'])
                    ctr = ctr + 1
                    if ctr == 60:
                        self.rpt.write(NL)
                        self.rpt.write(' '*81)
                    elif ctr % 10 == 0:
                        self.rpt.write(' ')

                self.rpt.write(NL)

            self.rpt.write('-'*25)
            self.rpt.write(NL)


class general:
    def __init__(self, rpt):
        self.rpt = rpt

    def header_type(self, rpt_typ, conn, event, name, startdate, enddate):
        self.rpt.write('-'*100 + NL)
        self.rpt.write('Analysis Type:'.ljust(HDR_LEN, ' ') + rpt_typ + NL)
        if rpt_typ == 'Event':
            qry_text = qry.event_summary(event)
            rs = pd.read_sql(qry_text, conn).values.tolist()
            self.rpt.write('Event Name:'.ljust(HDR_LEN, ' ') + event + NL)
            self.rpt.write('Event Date:'.ljust(HDR_LEN, ' ') + rs[0][1] + NL)
            self.rpt.write('Rounds:'.ljust(HDR_LEN, ' ') + str(int(rs[0][2])) + NL)
            self.rpt.write('Players:'.ljust(HDR_LEN, ' ') + str(int(rs[0][3])) + NL)
        elif rpt_typ == 'Player':
            self.rpt.write('Player Name:'.ljust(HDR_LEN, ' ') + ' '.join(name) + NL)
            self.rpt.write('Games Between:'.ljust(HDR_LEN, ' ') + startdate + ' - ' + enddate + NL)
        self.rpt.write(NL)

    def header_info(self, pgn, engine, depth):
        dte = dt.datetime.now().strftime('%m/%d/%Y')
        if pgn:
            self.rpt.write('File Name:'.ljust(HDR_LEN, ' ') + pgn + NL)
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
        self.rpt.write(NL)
        self.rpt.write(NL)

    def player_key(self):
        self.rpt.write('PLAYER KEY' + NL)
        self.rpt.write('-'*100 + NL)
        self.rpt.write('EVM:'.ljust(PK_LEN, ' '))
        self.rpt.write('Equal Value Match; moves with an evaluation that matches the best engine evaluation' + NL)
        self.rpt.write('ACPL:'.ljust(PK_LEN, ' '))
        self.rpt.write('Average Centipawn Loss; sum of total centipawn loss divided by the number of moves' + NL)
        self.rpt.write('SDCPL:'.ljust(PK_LEN, ' '))
        self.rpt.write('Standard Deviation Centipawn Loss; standard deviation of centipawn loss values from each move played' + NL)
        self.rpt.write('Score:'.ljust(PK_LEN, ' '))
        self.rpt.write('Game Score; measurement of how accurately the game was played, ranges from 0 to 100' + NL)
        self.rpt.write('ROI:'.ljust(PK_LEN, ' '))
        self.rpt.write('Raw Outlier Index; normalized Score value where 50 represents the mean and each increment of 5 is one standard deviation from the mean' + NL)
        self.rpt.write(' '*PK_LEN + 'An asterisk (*) following an ROI value indicates a situation that deserves extra scrutiny' + NL)
        self.rpt.write(NL)

    def game_key(self):
        self.rpt.write('GAME KEY' + NL)
        self.rpt.write('-'*100 + NL)
        self.rpt.write('(Player Name) (Elo) (Scored Moves)' + NL)
        self.rpt.write(' (Round)(Color) (Result) (Opp) (Opp Rating): (EVM/Turns = EVM%) (ACPL) (SDCPL) (Score) (ROI) (game trace)' + NL)
        self.rpt.write('Game trace key: b = Book move; M = EV match; 0 = inferior move; e = eliminated because one side far ahead, t = Tablebase hit' + NL)
        self.rpt.write(NL)
