for f in logs/*/; do python -m logalyzer.run.convert_to_sessions logs/$f/search.log.$1 >> logs/search-sessions.log.$1.jsonl; done
