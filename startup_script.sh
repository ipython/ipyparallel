#!/usr/bin/env bash
sudo apt update
sudo apt install -y python3-pip
sudo apt install -y python3-venv
pip3 install asv
sudo touch /etc/startup_script_finished