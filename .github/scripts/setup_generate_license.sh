sudo apt-get update && sudo apt-get install python3 -y
curl https://bootstrap.pypa.io/pip/3.5/get-pip.py | sudo -H python3
../../check_test_suite.py && travis_terminate 0 || echo 'Continuing setup'
pip3 install wheel  # install wheel first explicitly
pip3 install --upgrade pyyaml