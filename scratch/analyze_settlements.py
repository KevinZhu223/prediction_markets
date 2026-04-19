import re
from collections import defaultdict

log_text = """
Settlement: KXTEMPNYCH-26APR1005-T40.99 (YES) qty=1 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1006-T41.99 (YES) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1008-T42.99 (YES) qty=50 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR100830-30 (NO) qty=100 payout_per_contract=1.00 paid $100.00
Settlement: KXTEMPNYCH-26APR1009-T44.99 (YES) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR101000-00 (NO) qty=100 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1011-T51.99 (YES) qty=30 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1012-T55.99 (YES) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1016-T61.99 (YES) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR102130-30 (YES) qty=100 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1106-T57.99 (YES) qty=30 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1108-T56.99 (YES) qty=30 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1108-T55.99 (YES) qty=30 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1108-T52.99 (YES) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1108-T53.99 (YES) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXWTAMATCH-26APR11ANDRUS-AND (NO) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR111515-15 (NO) qty=100 payout_per_contract=0.00 paid $0.00
Settlement: KXNCAABBGAME-26APR111500UCLARSK-RSK (YES) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXNCAABBGAME-26APR111500SGELAL-LAL (YES) qty=20 payout_per_contract=1.00 paid $20.00
Settlement: KXBTC15M-26APR112200-00 (YES) qty=100 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR112215-15 (YES) qty=100 payout_per_contract=1.00 paid $100.00
Settlement: KXATPCHALLENGERMATCH-26APR11DANLEE-DAN (NO) qty=30 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR112230-30 (NO) qty=100 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1200-T52.99 (YES) qty=30 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR120700-00 (YES) qty=100 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1208-T45.99 (YES) qty=30 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1209-T48.99 (YES) qty=30 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1210-T50.99 (YES) qty=30 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1214-T56.99 (YES) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXMAMDANIMENTION-26APR12B-WORK (NO) qty=30 payout_per_contract=1.00 paid $30.00
Settlement: KXTEMPNYCH-26APR1216-T53.99 (YES) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1218-T51.99 (YES) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR121815-15 (YES) qty=200 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR122045-45 (NO) qty=100 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR122130-30 (NO) qty=100 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR130430-30 (YES) qty=100 payout_per_contract=1.00 paid $100.00
Settlement: KXATPSETWINNER-26APR13NAGCER-1-CER (NO) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1308-T55.99 (YES) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXATPMATCH-26APR13NAGCER-CER (NO) qty=30 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR131000-00 (NO) qty=200 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1310-T59.99 (YES) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR131030-30 (YES) qty=100 payout_per_contract=1.00 paid $100.00
Settlement: KXBTC15M-26APR131100-00 (NO) qty=100 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR131115-15 (NO) qty=100 payout_per_contract=1.00 paid $100.00
Settlement: KXBTC15M-26APR131130-30 (NO) qty=100 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR131145-45 (YES) qty=100 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR131545-45 (NO) qty=100 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR131600-00 (NO) qty=100 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR131830-30 (NO) qty=200 payout_per_contract=0.00 paid $0.00
Settlement: KXATPCHALLENGERMATCH-26APR13DRABIG-DRA (NO) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR140315-15 (NO) qty=200 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR140445-45 (NO) qty=100 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1405-T66.99 (YES) qty=20 payout_per_contract=1.00 paid $20.00
Settlement: KXATPMATCH-26APR14COBDED-COB (NO) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR141015-15 (YES) qty=115 payout_per_contract=1.00 paid $115.00
Settlement: KXBTC15M-26APR141100-00 (YES) qty=100 payout_per_contract=0.00 paid $0.00
Settlement: KXBTC15M-26APR141115-15 (YES) qty=100 payout_per_contract=0.00 paid $0.00
Settlement: KXTRUMPSAY-26APR20-RIGG (NO) qty=50 payout_per_contract=0.00 paid $0.00
Settlement: KXTRUMPSAY-26APR20-WIND (YES) qty=30 payout_per_contract=1.00 paid $30.00
Settlement: KXTEMPNYCH-26APR1501-T67.99 (NO) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1502-T73.99 (NO) qty=30 payout_per_contract=1.00 paid $30.00
Settlement: KXTEMPNYCH-26APR1503-T66.99 (NO) qty=30 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1505-T70.99 (YES) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXTEMPNYCH-26APR1506-T69.99 (YES) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXHIGHDEN-26APR14-T65 (YES) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXATPMATCH-26APR15BORETC-ETC (NO) qty=20 payout_per_contract=1.00 paid $20.00
Settlement: KXATPCHALLENGERMATCH-26APR15DELEST-DEL (NO) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXWTI-26APR15-T94.99 (NO) qty=30 payout_per_contract=1.00 paid $30.00
Settlement: KXTEMPNYCH-26APR1610-T80.99 (YES) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXATPCHALLENGERMATCH-26APR16CRAKEN-CRA (NO) qty=20 payout_per_contract=1.00 paid $20.00
Settlement: KXATPCHALLENGERMATCH-26APR16DELROD-DEL (NO) qty=20 payout_per_contract=0.00 paid $0.00
Settlement: KXHIGHLAX-26APR15-T73 (YES) qty=30 payout_per_contract=0.00 paid $0.00
Settlement: KXWTAMATCH-26APR16BAPSHY-BAP (NO) qty=20 payout_per_contract=1.00 paid $20.00
Settlement: KXWTI-26APR16-T85.99 (NO) qty=30 payout_per_contract=0.00 paid $0.00
Settlement: KXITFMATCH-26APR17KOUHEN-KOU (NO) qty=30 payout_per_contract=0.00 paid $0.00
Settlement: KXATPMATCH-26APR17SHAMOL-MOL (NO) qty=30 payout_per_contract=0.00 paid $0.00
"""

stats = defaultdict(lambda: {"wins": 0, "losses": 0, "total_paid": 0.0, "total_qty": 0})

for line in log_text.strip().split("\n"):
    match = re.search(r"Settlement: ([\w-]+).*\((\w+)\) qty=(\d+) payout_per_contract=([\d.]+) paid \$([\d.]+)", line)
    if match:
        market_full = match.group(1)
        strategy = market_full.split("-")[0]
        side = match.group(2)
        qty = int(match.group(3))
        payout = float(match.group(4))
        paid = float(match.group(5))
        
        stats[strategy]["total_qty"] += qty
        stats[strategy]["total_paid"] += paid
        if payout > 0:
            stats[strategy]["wins"] += 1
        else:
            stats[strategy]["losses"] += 1

print(f"{'Strategy':<25} {'Wins':<5} {'Losses':<8} {'Win%':<6} {'Total Paid':<10}")
print("-" * 60)
for strategy, s in stats.items():
    win_pct = (s["wins"] / (s["wins"] + s["losses"])) * 100
    print(f"{strategy:<25} {s['wins']:<5} {s['losses']:<8} {win_pct:>5.1f}% ${s['total_paid']:>9.2f}")
