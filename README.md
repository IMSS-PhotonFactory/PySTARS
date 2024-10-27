# PySTARS

STARS Kernel Python edition

See also [STARS-Core repository](https://github.com/IMSS-PhotonFactory/STARS-Core)

## How to start server

```sh
# This opens port, 6057, which is configured in PyStars.cfg.
python3 PyStars.py
```

## How to connect

```sh
telnet localhost 6057
term1 stars
System help
System>term1 @help flgon flgoff loadaliases listaliases loadpermission loadreconnectablepermission listnodes getversion gettime hello disconnect
```

refs. [STARS turial](https://stars.kek.jp/doku.php?id=start#stars_tutorial)
