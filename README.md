# script-mcc-migration

Needs Python 3, with pymysql and requests installed

`pip install pymysql requests`

To run the script

`python3 migrate-ac-users.py`

When running it creates two files. "created-users" for succesfuly saved users, and "missing-users" for failed ones.

(Hace falta renovar el token de fury cuando indique, TOKEN al principio del archivo del script. Y si se corre desde la pc personal, tiene que estar conectada la vpn. Si se corta la vpn, conectar de vuelta, cancelar la corrida con CTRL-D y correr de vuelta)