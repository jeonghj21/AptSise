export FLASK_CONFIG=$HOME/dev/config_dev.py
$HOME/web/bin/python $HOME/dev/saleList_Batch.py -from_ym=$(date '+%Y%m' -d -3month) -to_ym=$(date '+%Y%m')
