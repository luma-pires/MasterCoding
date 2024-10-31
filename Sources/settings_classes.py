from dynaconf import Dynaconf
from Functions.system import System


base_path = System.get_project_path()
sett_classes = Dynaconf(
    core_loaders=["JSON"],
    settings_files=[
        f"{base_path}\\Sources\\bet365\\classes.json",
        f"{base_path}\\Sources\\betfair\\classes.json",
        f"{base_path}\\Sources\\betway\\classes.json"
    ]
)