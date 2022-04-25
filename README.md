# label-studio-workers

## Requirements

- [Python>=3.7](https://www.python.org/downloads/)

## Getting started

```shell
git clone https://github.com/bird-feeder/label-studio-workers.git
cd label-studio-workers

pip install -r requirements.txt

mv .env.example .env
nano .env  # or any other editor
```

```shell
PYTHON_BIN=$(which python)  # for pyenv: PYTHON_BIN=$(pyenv which python)

echo "[Unit]
Description=sync data
Requires=network.target

[Service]
Type=idle
User=$USER
WorkingDirectory=$PWD
ExecStart=$PYTHON_BIN sync_data.py
Restart=always

[Install]
WantedBy=multi-user.target" > sync_data.service

mv sync_data.service /etc/systemd/system/sync_data.service

systemctl daemon-reload
systemctl start sync_data.service
systemctl enable sync_data.service
systemctl status sync_data.service
```
