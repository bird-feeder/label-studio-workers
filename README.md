# label-studio-workers

**MOVED TO: https://github.com/bird-feeder/BirdFSD-YOLOv5/tree/main/birdfsd_yolov5/label_studio_helpers**


## Requirements

- [Python>=3.7](https://www.python.org/downloads/)

## Getting started[^1]

```shell
git clone https://github.com/bird-feeder/label-studio-workers.git
cd label-studio-workers

pip install -r requirements.txt

mv .env.example .env
nano .env  # or any other editor
```

```shell
PYTHON_BIN=$(which python)
# for pyenv, run: PYTHON_BIN=$(pyenv which python)
PATH_TO_S3_DATA_BUCKET="REPLACE_ME"

echo "[Unit]
Description=tasks data watchdog
Requires=network.target

[Service]
Type=idle
User=root
WorkingDirectory=$PWD
ExecStart=$PYTHON_BIN watchdog.py --root-data-folder $PATH_TO_S3_DATA_BUCKET
Restart=always

[Install]
WantedBy=multi-user.target" > tasks_data_watchdog.service
```

```shell
sudo mv tasks_data_watchdog.service /etc/systemd/system/tasks_data_watchdog.service

sudo systemctl daemon-reload
sudo systemctl start tasks_data_watchdog.service
sudo systemctl enable tasks_data_watchdog.service
systemctl status tasks_data_watchdog.service
```

[^1]: Only shows how to run the watchdog, and nothing else at the moment. Will be updated soon.
