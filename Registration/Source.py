
def dict_sources():

    from Sources.bet365.bet365 import bet365
    from Sources.betfair.betfair import Betfair
    from Sources.betway.betway import Betway

    return {'bet365': bet365, 'betfair': Betfair, 'betway': Betway}
