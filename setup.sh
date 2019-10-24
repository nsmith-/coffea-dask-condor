#!/usr/bin/env bash
source /cvmfs/sft.cern.ch/lcg/views/LCG_96python3/x86_64-centos7-gcc8-opt/setup.sh

# following https://aarongorka.com/blog/portable-virtualenv/
# someday maybe use https://github.com/pantsbuild/pex
NAME=coffeaenv
python -m venv --copies $NAME
source $NAME/bin/activate
python -m pip install setuptools pip --upgrade
python -m pip install dask distributed bokeh htcondor jump-consistent-hash --upgrade
python -m pip install https://github.com/CoffeaTeam/coffea/archive/master.zip

sed -i '40s/.*/VIRTUAL_ENV="$(cd "$(dirname "$(dirname "${BASH_SOURCE[0]}" )")" \&\& pwd)"/' $NAME/bin/activate
sed -i '1s/#!.*python$/#!\/usr\/bin\/env python/' $NAME/bin/*
sed -i '2a source /cvmfs/sft.cern.ch/lcg/views/LCG_96python3/x86_64-centos7-gcc8-opt/setup.sh' $NAME/bin/activate

tar -zcf coffeaenv.tar.gz coffeaenv
mkdir logs
