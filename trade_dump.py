import glob

logs = sorted(glob.glob('logs/*.log'))[-3:]
out = []
for log in logs:
    with open(log, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            if 'FILLED:' in line or 'Portfolio:' in line or 'equity=' in line:
                out.append(f"{log} -> {line.strip()}")

with open('trade_analysis_clean.txt', 'w', encoding='utf-8') as f:
    f.write("\n".join(out))
