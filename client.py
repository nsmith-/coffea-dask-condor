#!/usr/bin/env python3
from dask.distributed import Client, SchedulerPlugin, WorkerPlugin, get_worker
import os
import sys
import time


class WorkerJumpAssignment(SchedulerPlugin):
    def __init__(self, workers=[]):
        self._workers = workers

    def add_worker(self, scheduler=None, worker=None, **kwargs):
        try:
            # cluster shrank and now is regrowing, insert in first empty slot
            index = self._workers.index(None)
            self._workers[index] = worker
        except ValueError:
            # cluster is expanding
            self._workers.append(worker)

    def remove_worker(self, scheduler=None, worker=None, **kwargs):
        try:
            index = self._workers.index(worker)
            self._workers[index] = None
        except ValueError:
            # invalid state, what do we do?
            raise

    def get_jump_mapping(self):
        out = {}
        for i, worker in enumerate(self._workers):
            if all(w is None for w in self._workers[i:]):
                break
            out[i] = worker
        return out


class ConfigureXRootD(WorkerPlugin):
    def __init__(self, proxy_file=None):
        '''
        If proxy_file is None, look for it in default location
        '''
        file = os.environ.get('X509_USER_PROXY', '/tmp/x509up_u%d' % os.getuid())
        self._proxy = open(file, 'rb').read()

    def setup(self, worker):
        self._location = os.path.join(worker.local_directory, 'userproxy')
        with open(self._location, 'wb') as fout:
            fout.write(self._proxy)
        os.environ['X509_USER_PROXY'] = self._location
        os.environ['XRD_CONNECTIONWINDOW'] = '10'
        os.environ['XRD_STREAMTIMEOUT'] = '10'
        os.environ['XRD_TIMEOUTRESOLUTION'] = '2'
        os.environ['XRD_WORKERTHREADS'] = '4'
        os.environ['XRD_REQUESTTIMEOUT'] = '60'

    def teardown(self, worker):
        os.remove(self._location)
        del os.environ['X509_USER_PROXY']


class DistributeZipball(WorkerPlugin):
    def __init__(self, zipfile):
        self._fname = os.path.basename(zipfile)
        self._code = open(zipfile, 'rb').read()

    def setup(self, worker):
        self._location = os.path.join(worker.local_directory, self._fname)
        self._pathstr = os.path.join(self._location, self._fname.replace('.zip', ''))
        with open(self._location, 'wb') as fout:
            fout.write(self._code)
        sys.path.insert(0, self._pathstr)

    def teardown(self, worker):
        os.remove(self._location)
        sys.path.remove(self._pathstr)


class InstallPackage(WorkerPlugin):
    def __init__(self, name):
        self._name = name

    def setup(self, worker):
        import os, sys, subprocess
        installdir = os.path.join(os.path.dirname(worker.local_directory), '.local')
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--prefix', installdir, self._name])
        sitepackages = os.path.join(installdir, 'lib', 'python' + sys.version[:3], 'site-packages')
        if sitepackages not in sys.path:
            sys.path.insert(0, sitepackages)

    def teardown(self, worker):
        # trust that nanny will cleanup worker local directory
        pass


client = Client(os.environ['DASK_SCHEDULER'])

# one-time setup
if True:
    client.restart()
    user_proxy = ConfigureXRootD()
    client.register_worker_plugin(user_proxy, 'user_proxy')
    client.register_worker_plugin(InstallPackage('https://github.com/nsmith-/boostedhiggs/archive/master.zip'), 'boostedhiggs')
    client.register_worker_plugin(InstallPackage('https://github.com/dnoonan08/TTGamma_LongExercise/archive/Exercise.zip'), 'ttgamma')
    # newcoffea = DistributeZipball('/home/ncsmith/coffea/dist/coffea-0.6.23.zip')
    # client.register_worker_plugin(newcoffea, 'coffeaupdate')
    # jump_assignment = WorkerJumpAssignment()
    # def put(dask_scheduler=None):
    #     dask_scheduler.add_plugin(WorkerJumpAssignment(list(dask_scheduler.workers.keys())))

    # def get(dask_scheduler=None):
    #     for p in dask_scheduler.plugins:
    #         try:
    #             return p.get_jump_mapping()
    #         except AttributeError:
    #             pass

    # client.run_on_scheduler(put)

from coffea import processor
from coffea.processor.test_items import NanoTestProcessor

filelist = {
    "DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8": [
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/16B6B7CD-4310-A042-AB52-7DA8ADA22922.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/05884C27-75AD-D340-B515-7017F9655675.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/0CA4B9C4-805D-C148-8281-D615F9DE8541.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/12C1D5AD-DFFB-F547-A634-17FE8AAB84B1.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/0F49C966-5F44-3D4F-AADF-F820A2EBF8A9.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/1A9BA6F1-F51D-F342-BB5D-F0F3B17ED70E.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/168D358A-B3B2-6849-9EF4-D2B6791A26AA.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/26884FA0-B96A-1745-AA11-597C5168EF5E.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/1C3AC8F7-987B-4D40-B002-767A2C65835B.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/FC56B1DA-20B9-F14A-A2CF-2097B8095BEB.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/E3828699-7905-3142-A0A2-929E60406883.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/F690EE89-E028-C840-8C4B-3A3106316530.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/CFC96CE1-3B0B-3D45-8D85-C73EE0C8DAB3.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/DC937EFE-252C-814B-A4B2-743A368793D8.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/C1F8DBF7-4649-2F43-9E03-2161344D4663.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/C3DC24F1-8416-0240-AC54-D63A9760BF45.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/BB0AF882-527A-D641-91BE-4624222CCB17.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/C1450DF7-55A8-FE4F-B50B-9E844F9E103E.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/B08CCF2F-A193-9640-AAE4-82F75CE2B436.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/B4D431A5-B57B-1D40-89AD-B6CB0C9F05E2.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/AF265BB7-CF6C-8241-8DC2-F13BA8A9AD60.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/AF34E3F0-25B7-6644-B557-1428CF675FDC.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/A5702444-A58D-364F-BF6C-EF28C9C52344.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/AB329578-42CC-4746-A15D-08E70CD2554E.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/9F70ACE0-A9C2-494C-B0E5-42E7017ABF95.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/A1B3E169-6D65-E44E-B891-8F738CBB78AD.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/932CE866-A30E-F34D-B0D5-4C4CEAA06CB8.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/948182F2-9993-C74D-B2EA-1D6E0098AD61.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/8F3EEF08-F61E-4046-B140-B04B87602708.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/8FA629F5-385A-AD4A-BB6F-D0856E633712.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/81FFF806-71B3-CC44-AB43-714DBE4C9319.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/8E443B60-F9E9-5444-B37D-62902E68C0C3.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/7BF6D85F-EE22-D840-A435-8CB817098E86.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/80D5E58B-2B5C-0440-9A1F-B6E2772FD7BD.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/761BC07D-C19C-C841-84E4-2766F1DCB60B.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/7AC63348-51B1-9649-B824-D04898C5BA5B.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/698C3A61-1ADA-0446-A47D-D8192E2B5415.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/6BAFEC1D-F9BD-8C48-8A2C-D00229113414.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/60C68FA7-5C29-2740-8242-E519DA6F5F10.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/6827CFDC-89DC-9147-98F8-B25D649B5857.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/5851642F-9C54-FC4E-A8B0-DEA3A10646E4.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/5AE16A97-FA54-0C45-8EDC-C1AA89D5B054.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/51F7E9BD-25F0-D941-BD9A-7F24FD6B1B04.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/572AD18F-BFA0-174E-9D28-0F75F965C147.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/4989E9FC-CD00-D64C-8527-9AD2FF2546A8.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/49CF353A-3303-ED44-8A65-994191042C99.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/43744293-FC6D-D24E-8BB5-DEB0F975AAF5.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/4966A0E8-CC00-BA4D-AEFF-F37332AC1096.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/3C6B053B-C36F-BA4B-8A44-1BC2C291DC8D.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/425C6243-B617-5347-B015-F0908D757828.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/2A9A7EDE-2249-2C44-AF6D-E44B83E8CBDF.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/3C0F69F9-2D31-6646-A1B0-FE021BE707C8.root",
        "root://cmsxrootd.fnal.gov//store/mc/RunIIAutumn18NanoAODv5/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/NANOAODSIM/Nano1June2019_102X_upgrade2018_realistic_v19-v1/110000/274599AC-1636-3641-B09F-ECA42B8F63A4.root",
    ],
}

config = {
    'client': client,
    'compression': 1,
    'savemetrics': 1,
    # 'xrootdconfig': {
    #     'chunkbytes': 1024*128,
    #     'limitbytes': 200 * 1024**2
    # },
}
chunksize = 160000

if True:
    tic = time.time()
    res = processor.run_uproot_job(filelist, 'Events', NanoTestProcessor(), processor.dask_executor, config, chunksize=chunksize, maxchunks=1)
    toc = time.time()

    print("Dask client:", client)
    print("Total time: %.0f" % (toc - tic))
    print("Events / s / thread: {:,.0f}".format(res[1]['entries'].value / res[1]['processtime'].value))
    print("Bytes / s / thread: {:,.0f}".format(res[1]['bytesread'].value / res[1]['processtime'].value))
    print("Events / s: {:,.0f}".format(res[1]['entries'].value / (toc - tic)))
    print("Bytes / s: {:,.0f}".format(res[1]['bytesread'].value / (toc - tic)))

    from coffea.util import save
    save(res, 'runX.coffea')
