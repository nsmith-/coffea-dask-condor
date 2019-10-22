from dask.distributed import Client
import os

client = Client(os.environ['DASK_SCHEDULER'])

from coffea import processor
from coffea.processor.test_items import NanoTestProcessor

filelist = {
  "GluGluHToBB_M125_13TeV_powheg_pythia8": [
      "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv4/GluGluHToBB_M125_13TeV_powheg_pythia8/NANOAODSIM/Nano14Dec2018_102X_upgrade2018_realistic_v16-v1/120000/5B168775-2647-1242-9505-20AE6DD3BC8C.root",
      "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv4/GluGluHToBB_M125_13TeV_powheg_pythia8/NANOAODSIM/Nano14Dec2018_102X_upgrade2018_realistic_v16-v1/120000/EE0850E4-AE54-A740-88E1-575A9E728C8D.root",
      "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv4/GluGluHToBB_M125_13TeV_powheg_pythia8/NANOAODSIM/Nano14Dec2018_102X_upgrade2018_realistic_v16-v1/120000/44C33646-5450-4B49-80D0-08BEB77B52C0.root",
      "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv4/GluGluHToBB_M125_13TeV_powheg_pythia8/NANOAODSIM/Nano14Dec2018_102X_upgrade2018_realistic_v16-v1/60000/A46064BF-6B98-D84F-8503-AAE9FB2171CE.root",
      "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv4/GluGluHToBB_M125_13TeV_powheg_pythia8/NANOAODSIM/Nano14Dec2018_102X_upgrade2018_realistic_v16-v1/60000/DE40B611-094E-0B4C-A3AB-CF4D1FC60B14.root",
      "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv4/GluGluHToBB_M125_13TeV_powheg_pythia8/NANOAODSIM/Nano14Dec2018_102X_upgrade2018_realistic_v16-v1/60000/9F5D12E9-9B0F-0742-85DB-EA88B000E33C.root",
      "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv4/GluGluHToBB_M125_13TeV_powheg_pythia8/NANOAODSIM/Nano14Dec2018_102X_upgrade2018_realistic_v16-v1/60000/1AA76CD2-856C-294C-A951-08A2369D26CF.root",
      "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv4/GluGluHToBB_M125_13TeV_powheg_pythia8/NANOAODSIM/Nano14Dec2018_102X_upgrade2018_realistic_v16-v1/60000/F2FAEEDD-C742-B947-8380-B04DF28D5796.root",
      "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv4/GluGluHToBB_M125_13TeV_powheg_pythia8/NANOAODSIM/Nano14Dec2018_102X_upgrade2018_realistic_v16-v1/120000/504FCCC3-BBAB-F04F-A53B-581528DCAC19.root",
      "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv4/GluGluHToBB_M125_13TeV_powheg_pythia8/NANOAODSIM/Nano14Dec2018_102X_upgrade2018_realistic_v16-v1/60000/793ECD9E-CDFD-784C-A21E-DFC66BC9E3F7.root",
      "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv4/GluGluHToBB_M125_13TeV_powheg_pythia8/NANOAODSIM/Nano14Dec2018_102X_upgrade2018_realistic_v16-v1/60000/033FC252-6668-9344-8B32-D5BC864D9C05.root",
      "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv4/GluGluHToBB_M125_13TeV_powheg_pythia8/NANOAODSIM/Nano14Dec2018_102X_upgrade2018_realistic_v16-v1/60000/28F03936-94AD-3945-B8EE-2C2219497CB8.root"
    ],
}

res = processor.run_uproot_job(filelist, 'Events', NanoTestProcessor(), processor.dask_executor, {'client': client, 'compression': 1, 'savemetrics': True})
