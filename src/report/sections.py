import datetime as dt
import math

import pandas as pd

import queries as qry

NL = '\n'
EV_LEN = 35
HDR_LEN = 20
PK_LEN = 10


def game_traces(rpt, conn, event, name, startdate, enddate):
    rpt.write('-'*25)
    rpt.write(NL)

    if event:
        qry_text = qry.event_playergames(event)
    else:
        qry_text = qry.player_playergames(name, startdate, enddate)
    player_rs = pd.read_sql(qry_text, conn)
    for i, player in player_rs.iterrows():
        rpt.write(player['Name'])
        rt = int(math.floor(player['Rating']/100)*100)
        rpt.write(' ' + str(player['Rating']))
        rpt.write(' ' + str(player['ScoredMoves']))
        rpt.write(NL)

        if event:
            qry_text = qry.event_playeropp(player['Name'], event)
        else:
            qry_text = qry.player_playeropp(player['Name'], startdate, enddate)
        game_rs = pd.read_sql(qry_text, conn)
        for ii, game in game_rs.iterrows():
            g_id = int(game['GameID'])
            rd = str(int(game['RoundNum']))
            if len(rd) == 1:
                rpt.write(' ')
                rpt.write(rd)
            elif len(rd) > 2:
                rpt.write('  ')
            else:
                rpt.write(rd)

            rpt.write(game['Color'] + ' ')
            rpt.write(game['Result'] + ' ')
            rpt.write(game['OppName'][0:21].ljust(20, ' '))
            rpt.write(str(game['OppRating']) + ':  ')

            evm = str(game['EVM']).ljust(3, ' ') + ' / ' + str(game['ScoredMoves']).ljust(3, ' ') + ' = '
            evm = evm + '{:3.0f}'.format(100*game['EVM']/game['ScoredMoves']) + '%'
            rpt.write(evm + '  ')

            acpl = '{:.4f}'.format(game['ACPL'])
            rpt.write(acpl.ljust(8, ' '))

            sdcpl = '{:.4f}'.format(game['SDCPL'])
            rpt.write(sdcpl.ljust(8, ' '))

            score = '{:.2f}'.format(game['Score'])
            rpt.write(score.ljust(7, ' '))

            z_qry = qry.roi_calc(agg='Game', src='Control', tc='Classical', rating=rt)
            z_rs = pd.read_sql(z_qry, conn).values.tolist()
            z_score = (game['Score'] - z_rs[0][0])/z_rs[0][1]
            roi = '{:.1f}'.format(50 + z_score*5)
            roi = roi + '*' if (50 + z_score*5) >= 70 else roi
            rpt.write(roi.ljust(7, ' '))

            # moves
            c = 'White' if game['Color'] == 'w' else 'Black'
            qry_text = qry.game_trace(g_id, c)
            moves_rs = pd.read_sql(qry_text, conn)
            ctr = 0
            for iii, mv in moves_rs.iterrows():
                rpt.write(mv['MoveTrace'])
                ctr = ctr + 1
                if ctr == 60:
                    rpt.write(NL)
                    rpt.write(' '*81)
                elif ctr % 10 == 0:
                    rpt.write(' ')

            rpt.write(NL)

        rpt.write('-'*25)
        rpt.write(NL)


def key_stats(rpt, conn, event, name, startdate, enddate):
    if event:
        rpt.write('Whole-event statistics:' + NL)
    else:
        rpt.write('Whole-sample statistics:' + NL)

    rpt.write('-'*25 + NL)

    if event:
        rpt.write('Average rating by game:'.ljust(EV_LEN, ' '))
        qry_text = qry.event_avgrating(event)
    else:
        rpt.write('Average opponent rating:'.ljust(EV_LEN, ' '))
        qry_text = qry.player_avgrating(name, startdate, enddate)
    rs = pd.read_sql(qry_text, conn).values.tolist()
    rt = math.floor(int(rs[0][0])/100)*100  # TODO: Sync the rating with the player summary section
    rpt.write(str(int(rs[0][0])) + '; ')
    rpt.write('min ' + str(int(rs[0][1])) + ', ')
    rpt.write('max ' + str(int(rs[0][2])) + NL)

    if event:
        qry_text = qry.event_totalmoves(event)
    else:
        qry_text = qry.player_totalmoves(name, startdate, enddate)
    rs = pd.read_sql(qry_text, conn).values.tolist()
    mvs = int(rs[0][0])

    if event:
        qry_text = qry.event_scoredmoves(event)
    else:
        qry_text = qry.player_scoredmoves(name, startdate, enddate)
    rs = pd.read_sql(qry_text, conn).values.tolist()

    if event:
        rpt.write('Average rating for scored moves:'.ljust(EV_LEN, ' ') + str(int(rs[0][0])) + NL)
    rpt.write('Scored Moves percentage:'.ljust(EV_LEN, ' '))
    rpt.write(str(int(rs[0][1])) + ' / ' + str(mvs) + ' = ' + '{:.2f}'.format(100*int(rs[0][1])/mvs) + '%' + NL)
    rpt.write(NL)

    qry_text = qry.max_eval()
    mx_ev = pd.read_sql(qry_text, conn).values.tolist()
    mx_ev = str(int(mx_ev[0][0]))
    rpt.write(f'A move is Scored if it does not meet any of the following: Theoretical move, tablebase move, or one side is up by {mx_ev} centipawns' + NL)
    rpt.write(NL)

    rpt.write('Total T1:'.ljust(EV_LEN, ' '))
    rpt.write(str(int(rs[0][2])) + ' / ' + str(int(rs[0][1])) + ' = ' + '{:.2f}'.format(100*int(rs[0][2])/int(rs[0][1])) + '%' + NL)
    rpt.write('Total T2:'.ljust(EV_LEN, ' '))
    rpt.write(str(int(rs[0][3])) + ' / ' + str(int(rs[0][1])) + ' = ' + '{:.2f}'.format(100*int(rs[0][3])/int(rs[0][1])) + '%' + NL)
    rpt.write('Total T3:'.ljust(EV_LEN, ' '))
    rpt.write(str(int(rs[0][4])) + ' / ' + str(int(rs[0][1])) + ' = ' + '{:.2f}'.format(100*int(rs[0][4])/int(rs[0][1])) + '%' + NL)
    rpt.write('Total T4:'.ljust(EV_LEN, ' '))
    rpt.write(str(int(rs[0][5])) + ' / ' + str(int(rs[0][1])) + ' = ' + '{:.2f}'.format(100*int(rs[0][5])/int(rs[0][1])) + '%' + NL)
    rpt.write('Total T5:'.ljust(EV_LEN, ' '))
    rpt.write(str(int(rs[0][6])) + ' / ' + str(int(rs[0][1])) + ' = ' + '{:.2f}'.format(100*int(rs[0][6])/int(rs[0][1])) + '%' + NL)
    rpt.write('Total ACPL:'.ljust(EV_LEN, ' '))
    rpt.write('{:.4f}'.format(rs[0][7]) + NL)
    rpt.write('Total SDCPL:'.ljust(EV_LEN, ' '))
    rpt.write('{:.4f}'.format(rs[0][8]) + NL)
    rpt.write(NL)

    if event:
        qry_text = qry.event_totalscore(event)
    else:
        qry_text = qry.player_totalscore(name, startdate, enddate)
    rs = pd.read_sql(qry_text, conn).values.tolist()

    if event:
        rpt.write('Overall event score:'.ljust(EV_LEN, ' '))
    else:
        rpt.write('Overall player score:'.ljust(EV_LEN, ' '))
    rpt.write('{:2.2f}'.format(rs[0][0]) + NL)

    z_qry = qry.roi_calc(agg='Event', src='Control', tc='Classical', rating=rt)
    z_rs = pd.read_sql(z_qry, conn).values.tolist()
    z_score = (rs[0][0] - z_rs[0][0])/z_rs[0][1]
    roi = '{:.1f}'.format(50 + z_score*5)
    roi = roi + '*' if (50 + z_score*5) >= 70 else roi

    if event:
        rpt.write('Overall event ROI:'.ljust(EV_LEN, ' '))
    else:
        rpt.write('Overall player ROI:'.ljust(EV_LEN, ' '))
    rpt.write(roi + NL)

    rpt.write(NL)
    rpt.write(NL)


def game_key(rpt):
    rpt.write('GAME KEY' + NL)
    rpt.write('-'*100 + NL)
    rpt.write('(Player Name) (Elo) (Scored Moves)' + NL)
    rpt.write(' (Round)(Color) (Result) (Opp) (Opp Rating): (EVM/Turns = EVM%) (ACPL) (SDCPL) (Score) (ROI) (game trace)' + NL)
    rpt.write('Game trace key: b = Book move; M = EV match; 0 = inferior move; e = eliminated because one side far ahead, t = Tablebase hit' + NL)
    rpt.write(NL)


def header_info(rpt, pgn, engine, depth):
    dte = dt.datetime.now().strftime('%m/%d/%Y')
    if pgn:
        rpt.write('File Name:'.ljust(HDR_LEN, ' ') + pgn + NL)
    rpt.write('Engine Name:'.ljust(HDR_LEN, ' ') + engine + NL)
    rpt.write('Depth:'.ljust(HDR_LEN, ' ') + str(depth) + NL)
    rpt.write('Report Date:'.ljust(HDR_LEN, ' ') + dte + NL)
    rpt.write(NL)
    rpt.write(NL)


def header_type(rpt, rpt_typ, conn, event, name, startdate, enddate):
    rpt.write('-'*100 + NL)
    rpt.write('Analysis Type:'.ljust(HDR_LEN, ' ') + rpt_typ + NL)
    if rpt_typ == 'Event':
        qry_text = qry.event_summary(event)
        rs = pd.read_sql(qry_text, conn).values.tolist()
        rpt.write('Event Name:'.ljust(HDR_LEN, ' ') + event + NL)
        rpt.write('Event Date:'.ljust(HDR_LEN, ' ') + rs[0][1] + NL)
        rpt.write('Rounds:'.ljust(HDR_LEN, ' ') + str(int(rs[0][2])) + NL)
        rpt.write('Players:'.ljust(HDR_LEN, ' ') + str(int(rs[0][3])) + NL)
    elif rpt_typ == 'Player':
        rpt.write('Player Name:'.ljust(HDR_LEN, ' ') + ' '.join(name) + NL)
        rpt.write('Games Between:'.ljust(HDR_LEN, ' ') + startdate + ' - ' + enddate + NL)
    else:
        pass
    rpt.write(NL)


def player_key(rpt):
    rpt.write('PLAYER KEY' + NL)
    rpt.write('-'*100 + NL)
    rpt.write('EVM:'.ljust(PK_LEN, ' '))
    rpt.write('Equal Value Match; moves with an evaluation that matches the best engine evaluation' + NL)
    rpt.write('ACPL:'.ljust(PK_LEN, ' '))
    rpt.write('Average Centipawn Loss; sum of total centipawn loss divided by the number of moves' + NL)
    rpt.write('SDCPL:'.ljust(PK_LEN, ' '))
    rpt.write('Standard Deviation Centipawn Loss; standard deviation of centipawn loss values from each move played' + NL)
    rpt.write('Score:'.ljust(PK_LEN, ' '))
    rpt.write('Game Score; measurement of how accurately the game was played, ranges from 0 to 100' + NL)
    rpt.write('ROI:'.ljust(PK_LEN, ' '))
    rpt.write('Raw Outlier Index; normalized Score value where 50 represents the mean and each increment of 5 is one standard deviation from the mean' + NL)
    rpt.write(' '*PK_LEN + 'An asterisk (*) following an ROI value indicates a situation that deserves extra scrutiny' + NL)
    rpt.write(NL)


def player_summary(rpt, conn, event, name, startdate, enddate):
    player_len = 30
    elo_len = 7
    rec_len = 16
    perf_len = 8
    evm_len = 24
    acpl_len = 10
    sdcpl_len = 10
    score_len = 10
    roi_len = 8

    rpt.write('Player Name'.ljust(player_len, ' '))
    rpt.write('Elo'.ljust(elo_len, ' '))
    rpt.write('Record'.ljust(rec_len, ' '))
    rpt.write('Perf'.ljust(perf_len, ' '))
    rpt.write('EVM / Turns = Pcnt'.ljust(evm_len, ' '))
    rpt.write('ACPL'.ljust(acpl_len, ' '))
    rpt.write('SDCPL'.ljust(sdcpl_len, ' '))
    rpt.write('Score'.ljust(score_len, ' '))
    rpt.write('ROI'.ljust(roi_len, ' '))
    rpt.write('Opp EVM / Turns = Pcnt'.ljust(24, ' '))
    rpt.write('OppACPL'.ljust(acpl_len, ' '))
    rpt.write('OppSDCPL'.ljust(sdcpl_len, ' '))
    rpt.write('OppScore'.ljust(score_len, ' '))
    # rpt.write('OppROI'.ljust(roi_len, ' '))
    rpt.write(NL)
    rpt.write('-'*184)
    rpt.write(NL)

    if event:
        qry_text = qry.event_playersummary(event)
    else:
        qry_text = qry.player_playersummary(name, startdate, enddate)
    rs = pd.read_sql(qry_text, conn)
    for idx, player in rs.iterrows():
        rpt.write(player['Name'][0:player_len].ljust(player_len, ' '))
        rt = int(math.floor(player['Rating']/100)*100)
        rpt.write(str(player['Rating']).ljust(elo_len, ' '))

        if player['GamesPlayed'] < 10:
            gp_len = 3
        elif player['GamesPlayed'] >= 100:
            gp_len = 5
        else:
            gp_len = 4
        rec = str(player['Record']).ljust(gp_len, ' ') + ' / ' + str(player['GamesPlayed'])
        rpt.write(rec.ljust(rec_len, ' '))

        perf = str(player['Perf'])
        perf = '+' + perf if perf[0] != '-' else perf
        rpt.write(perf.ljust(perf_len, ' '))

        evm = str(player['EVM']) .ljust(4, ' ') + ' / ' + str(player['ScoredMoves']).ljust(4, ' ') + ' = '
        evm = evm + '{:2.1f}'.format(100*player['EVM']/player['ScoredMoves']) + '%'
        rpt.write(evm.ljust(evm_len, ' '))

        acpl = '{:.4f}'.format(player['ACPL'])
        rpt.write(acpl.ljust(acpl_len, ' '))

        sdcpl = '{:.4f}'.format(player['SDCPL'])
        rpt.write(sdcpl.ljust(sdcpl_len, ' '))

        score = '{:.2f}'.format(player['Score'])
        rpt.write(score.ljust(score_len, ' '))

        z_qry = qry.roi_calc(agg='Event', src='Control', tc='Classical', rating=rt)
        z_rs = pd.read_sql(z_qry, conn).values.tolist()
        z_score = (player['Score'] - z_rs[0][0])/z_rs[0][1]
        roi = '{:.1f}'.format(50 + z_score*5)
        roi = roi + '*' if (50 + z_score*5) >= 70 else roi
        rpt.write(roi.ljust(roi_len, ' '))

        oppevm = str(player['OppEVM']) .ljust(4, ' ') + ' / ' + str(player['OppScoredMoves']).ljust(4, ' ') + ' = '
        oppevm = oppevm + '{:2.1f}'.format(100*player['OppEVM']/player['OppScoredMoves']) + '%'
        rpt.write(oppevm.ljust(evm_len, ' '))

        oppacpl = '{:.4f}'.format(player['OppACPL'])
        rpt.write(oppacpl.ljust(acpl_len, ' '))

        oppsdcpl = '{:.4f}'.format(player['OppSDCPL'])
        rpt.write(oppsdcpl.ljust(sdcpl_len, ' '))

        oppscore = '{:.2f}'.format(player['OppScore'])
        rpt.write(oppscore.ljust(score_len, ' '))

        # z_qry = qry.roi_calc(agg='Event', src='Control', tc='Classical', rating=rt)
        # z_rs = pd.read_sql(z_qry, conn).values.tolist()
        # z_score = (player['OppScore'] - z_rs[0][0])/z_rs[0][1]
        # opproi = '{:.1f}'.format(50 + z_score*5)
        # opproi = opproi + '*' if (50 + z_score*5) >= 70 else opproi
        # rpt.write(opproi)

        rpt.write(NL)

    rpt.write(NL)
    rpt.write(NL)


def scoring_desc(rpt, conn):
    qry_text = qry.max_eval()
    mx_ev = pd.read_sql(qry_text, conn).values.tolist()
    mx_ev = str(int(mx_ev[0][0]))
    rpt.write('MOVE SCORING' + NL)
    rpt.write('-'*25 + NL)
    rpt.write('A move is Scored if it does not meet any of the following:' + NL)
    rpt.write('Is theoretical opening move' + NL)
    rpt.write('Is a tablebase hit' + NL)
    rpt.write(f'The best engine evaluation is greater than {mx_ev} centipawns or a mate in N' + NL)
    rpt.write('The engine evaluation of the move played is a mate in N' + NL)
    rpt.write(NL)
    rpt.write(NL)
