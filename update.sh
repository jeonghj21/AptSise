export FLASK_CONFIG=$HOME/aptsise/config_prod.py
$HOME/web/bin/python $HOME/aptsise/saleList_Batch.py -from_ym=$(date '+%Y%m' -d -3month) -to_ym=$(date '+%Y%m')
