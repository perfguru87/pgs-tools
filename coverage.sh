coverage run ./tools/pgs-info
coverage run ./tools/pgs-ps
coverage run ./tools/pgs-bench
coverage run ./tools/pgs-top -d 1 -n 2
reset
coverage run ./tools/pgs-bench -t 1
coverage run ./tools/pgs-info -w tmp.html
rm tmp.html
coverage run ./lib/pgs_report.py
coverage run ./lib/pgs_repaggr.py
coverage report -m
