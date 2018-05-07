arg1 = $1
for filename in $arg1/*; do ./movetogcs.sh ${filename}; done
