from Managerr.Manager import Manager
from Config.settings import sett
from joblib import Parallel, delayed

if __name__ == '__main__':

    workers = sett.general.workers
    results = Parallel(n_jobs=len(workers))(delayed(Manager(sett).manage)(i) for i in workers)
    print(results)
