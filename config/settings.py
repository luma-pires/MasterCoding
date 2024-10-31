from dynaconf import Dynaconf


sett = Dynaconf(
    core_loaders=["JSON"],
    settings_files=[
        "sql.json",
        "general.json",
        "machines.json"
    ]
)
