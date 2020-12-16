export FLASK_CONFIG=$HOME/aptsise/config_prod.py
$HOME/web/bin/python $HOME/aptsise/saleList_Batch.py -from_ym=$(date '+%Y%m') -to_ym=$(date '+%Y%m')
